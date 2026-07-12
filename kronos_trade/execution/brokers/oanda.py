"""
kronos_trade/execution/brokers/oanda.py

OANDA v20 REST broker adapter.

Uses httpx directly against OANDA's v20 REST API — no extra SDK needed,
works natively on Mac with no Docker or Wine.

Instrument format:  KATS uses "AUDCHF" / "EURCHF"
                    OANDA uses "AUD_CHF" / "EUR_CHF"
Unit sizing:        OANDA uses units (positive = long, negative = short)
                    1 standard lot = 100,000 units
                    0.01 lots = 1,000 units (micro-lot equivalent)
                    Default: kats_cfg.oanda_units_per_k units per $1,000 equity

Native brackets:    OANDA supports takeProfitOnFill + stopLossOnFill in one
                    order — no separate exit orders needed.

Practice vs live:   Controlled by settings.oanda_practice (True = practice).
                    Practice server: api-fxpractice.oanda.com
                    Live server:     api-fxtrade.oanda.com

Trade mirroring:    After OANDA fills are confirmed, they can be mirrored to
                    an MT5/FTMO account via a copy-trade EA on the MT5 side.
"""
from __future__ import annotations

import asyncio
import json
import math
import re
from datetime import datetime, timedelta, timezone

import httpx
from loguru import logger

from kronos_trade.config import kats_cfg, settings
from kronos_trade.models import (
    AccountSnapshot, BracketOrder, BrokerName,
    Direction, OrderSide, OrderStatus, Position,
)
from .base import BrokerAdapter


# ── Symbol helpers ─────────────────────────────────────────────────────────────

def _to_oanda(symbol: str) -> str:
    """AUDCHF → AUD_CHF  (insert underscore between the two 3-char currencies)."""
    s = symbol.replace("/", "").replace("-", "").replace("_", "").upper()
    return f"{s[:3]}_{s[3:]}" if len(s) == 6 else s


def _from_oanda(symbol: str) -> str:
    """AUD_CHF → AUDCHF"""
    return symbol.replace("_", "")


def _parse_oanda_dt(s: str) -> datetime | None:
    """
    Parse an OANDA RFC3339 timestamp into a timezone-aware datetime.

    OANDA returns nanosecond precision, e.g. "2024-01-15T10:30:00.123456789Z".
    Python's datetime.fromisoformat() only handles up to microseconds (6 digits),
    so we truncate any extra fractional digits before parsing.
    """
    if not s:
        return None
    # Truncate fractional seconds beyond 6 digits (nanoseconds → microseconds)
    s = re.sub(r'(\.\d{6})\d+', r'\1', s)
    # Normalise Z suffix
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def _units_from_equity(equity: float, units_per_k: int) -> int:
    """
    Scale units linearly with equity.
    units_per_k is the target density at $1,000 equity — NOT a floor.
    A $20 account gets 20 units; a $10,000 account gets 10,000 units.
    """
    return max(1, int((equity / 1_000.0) * units_per_k))


class OANDAAdapter(BrokerAdapter):
    """
    Full OANDA v20 REST broker adapter.

    Bracket orders are submitted as a single market order with
    takeProfitOnFill and stopLossOnFill — OANDA manages exit orders
    server-side, so they survive a KATS restart.
    """

    def __init__(self) -> None:
        env          = "fxpractice" if settings.oanda_practice else "fxtrade"
        self._rest   = f"https://api-{env}.oanda.com/v3"
        self._stream = f"https://stream-{env}.oanda.com/v3"
        self._acct   = settings.oanda_account_id
        self._headers = {
            "Authorization":  f"Bearer {settings.oanda_api_token}",
            "Content-Type":   "application/json",
            "Accept-Datetime-Format": "RFC3339",
        }
        self._http: httpx.AsyncClient | None = None

        # Populated at connect() from /accounts/{id}/instruments
        self._tradeable:    set[str]         = set()   # internal format: "EURUSD"
        self._digits_map:   dict[str, int]   = {}      # "EUR_USD" → 5
        self._margin_rates: dict[str, float] = {}      # "EUR_USD" → 0.0333

        # Background task for OANDA transaction stream (TP/SL exit events)
        self._tx_task: asyncio.Task | None = None
        self._tx_stop: asyncio.Event       = asyncio.Event()

    # ── BrokerAdapter interface ────────────────────────────────────────────────

    @property
    def name(self) -> BrokerName:
        return BrokerName.OANDA

    @property
    def supported_symbols(self) -> set[str]:
        """
        Live set of tradeable instruments for this account.
        Empty until connect() has been called.
        Returns empty set (not None) so the router skips symbol filtering
        on the first startup call before connect finishes — matches base
        class behaviour for dynamic brokers.
        """
        return self._tradeable

    @property
    def stream_base(self) -> str:
        return self._stream

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def connect(self) -> None:
        self._http = httpx.AsyncClient(
            headers=self._headers,
            timeout=10.0,
        )
        # ── 1. Validate credentials via account summary ────────────────────────
        resp = await self._http.get(f"{self._rest}/accounts/{self._acct}/summary")
        resp.raise_for_status()
        info = resp.json()["account"]
        env_label = "PRACTICE" if settings.oanda_practice else "LIVE"
        logger.success(
            f"[oanda] connected [{env_label}] | "
            f"account={info['id']} "
            f"balance={info['balance']} {info['currency']} "
            f"NAV={info['NAV']}"
        )

        # ── 2. Fetch tradeable instruments for this account ────────────────────
        await self._fetch_instruments()

        # ── 3. Start background transaction stream (TP/SL exit logging) ───────
        self._tx_stop.clear()
        self._tx_task = asyncio.create_task(
            self._transaction_stream_loop(),
            name="oanda-tx-stream",
        )

    async def disconnect(self) -> None:
        self._tx_stop.set()
        if self._tx_task and not self._tx_task.done():
            self._tx_task.cancel()
            try:
                await self._tx_task
            except (asyncio.CancelledError, Exception):
                pass
            self._tx_task = None
        if self._http:
            await self._http.aclose()
            self._http = None
        logger.info("[oanda] disconnected")

    # ── Orders ─────────────────────────────────────────────────────────────────

    async def submit_bracket_order(self, order: BracketOrder) -> BracketOrder:
        instrument = _to_oanda(order.symbol)

        # Units: positive = buy (long), negative = sell (short)
        account          = await self.get_account()
        equity           = account.equity         if account else 10_000.0
        available_margin = account.buying_power   if account else equity * 0.5
        raw_units        = _units_from_equity(equity, kats_cfg.oanda_units_per_k)

        # Hard-cap units so the required margin never exceeds available margin.
        # margin_per_unit = entry_price × margin_rate  (e.g. 1.27 × 0.0333 ≈ $0.042)
        # We stay inside 90 % of available margin as a safety buffer.
        margin_rate      = self._margin_rates.get(instrument, 0.0333)
        ref_price        = order.entry_price or 1.0
        margin_per_unit  = ref_price * margin_rate
        if margin_per_unit > 0:
            max_by_margin = int((available_margin * 0.90) / margin_per_unit)
            if raw_units > max_by_margin:
                logger.info(
                    f"[oanda] capping {instrument} units "
                    f"{raw_units} → {max_by_margin} "
                    f"(margin available=${available_margin:.2f} "
                    f"rate={margin_rate:.2%})"
                )
                raw_units = max(1, max_by_margin)

        units = raw_units if order.side == OrderSide.BUY else -raw_units

        # Price precision: cached from connect-time instrument fetch
        digits = self._instrument_digits(instrument)

        # Submit a plain market order — no TP/SL on fill.
        #
        # OANDA validates takeProfitOnFill / stopLossOnFill against the *live*
        # order book at submission time, not at fill time.  For shorts this means
        # the TP (entry − atr×mult) must already be below the current ask — but
        # if the signal was built on a stale bar-close price and the market has
        # ticked up even slightly, OANDA rejects with LOSING_TAKE_PROFIT.
        #
        # Two-step approach:
        #   1. Submit market order (always fills cleanly).
        #   2. PATCH the opened trade with TP/SL anchored to the actual fill price
        #      → they are always on the correct side of the market.
        body: dict = {
            "order": {
                "type":        "MARKET",
                "instrument":  instrument,
                "units":       str(units),
                "timeInForce": "FOK",
                "positionFill": "DEFAULT",
            }
        }

        logger.info(
            f"[oanda] {'BUY' if units > 0 else 'SELL'} {abs(units)} units {instrument} "
            f"sl={order.stop_loss} tp={order.take_profit}"
        )

        try:
            resp = await self._http.post(
                f"{self._rest}/accounts/{self._acct}/orders",
                json=body,
            )
            resp.raise_for_status()
            data = resp.json()
        except httpx.HTTPStatusError as exc:
            err = exc.response.text
            order.status        = OrderStatus.REJECTED
            order.reject_reason = f"OANDA order rejected: {err}"
            logger.error(f"[oanda] {order.reject_reason}")
            return order
        except Exception as exc:
            order.status        = OrderStatus.REJECTED
            order.reject_reason = f"OANDA request error: {exc}"
            logger.error(f"[oanda] {order.reject_reason}")
            return order

        # Check for order rejection in the response body
        if "orderCancelTransaction" in data:
            reason = data["orderCancelTransaction"].get("reason", "unknown")
            order.status        = OrderStatus.REJECTED
            order.reject_reason = f"OANDA cancelled order: {reason}"
            logger.warning(f"[oanda] {order.reject_reason}")
            return order

        fill              = data.get("orderFillTransaction", {})
        trade_id          = fill.get("tradeOpened", {}).get("tradeID", "")
        filled_price      = float(fill.get("price", order.entry_price))

        order.status          = OrderStatus.SUBMITTED
        order.broker_order_id = trade_id
        order.filled_price    = filled_price
        order.filled_at       = datetime.now(tz=timezone.utc)
        order.quantity        = abs(units)

        logger.success(
            f"[oanda] ✓ filled | tradeID={trade_id} "
            f"price={filled_price} units={units}"
        )

        # ── Step 2: attach TP/SL to the live trade ────────────────────────────
        # Recompute from the actual fill price so the levels are always on the
        # correct side regardless of any spread/staleness in the signal price.
        if trade_id and (order.stop_loss or order.take_profit):
            await self._patch_trade_brackets(
                trade_id    = trade_id,
                fill_price  = filled_price,
                order       = order,
                digits      = digits,
            )

        return order

    async def close_position(self, symbol: str) -> bool:
        instrument = _to_oanda(symbol)
        try:
            resp = await self._http.put(
                f"{self._rest}/accounts/{self._acct}/positions/{instrument}/close",
                json={"longUnits": "ALL", "shortUnits": "ALL"},
            )
            resp.raise_for_status()
            data = resp.json()
            closed = (
                data.get("longOrderFillTransaction", {}).get("units", "0") != "0"
                or data.get("shortOrderFillTransaction", {}).get("units", "0") != "0"
            )
            if closed:
                logger.info(f"[oanda] closed position for {instrument}")
            return closed
        except Exception as exc:
            logger.error(f"[oanda] close_position failed for {symbol}: {exc}")
            return False

    # ── Account & positions ────────────────────────────────────────────────────

    async def get_account(self) -> AccountSnapshot | None:
        try:
            resp = await self._http.get(
                f"{self._rest}/accounts/{self._acct}/summary"
            )
            resp.raise_for_status()
            d = resp.json()["account"]
            return AccountSnapshot(
                broker         = BrokerName.OANDA,
                equity         = float(d["NAV"]),
                cash           = float(d["balance"]),
                buying_power   = float(d["marginAvailable"]),
                daily_pnl      = float(d.get("unrealizedPL", 0)),
                unrealized_pnl = float(d.get("unrealizedPL", 0)),
            )
        except Exception as exc:
            logger.error(f"[oanda] get_account failed: {exc}")
            return None

    async def _get_trades_map(self) -> dict[str, dict]:
        """
        Fetch /openTrades and return a per-symbol dict with tradeID, entry price,
        stop_loss, and take_profit.  The openPositions endpoint does not expose
        per-trade TP/SL; openTrades does via takeProfitOrder.price /
        stopLossOrder.price.

        Used by get_positions() so the router receives enriched Position objects
        and can adopt pre-existing positions after a restart.
        """
        try:
            resp = await self._http.get(
                f"{self._rest}/accounts/{self._acct}/openTrades"
            )
            resp.raise_for_status()
            trades = resp.json().get("trades", [])
        except Exception as exc:
            logger.warning(f"[oanda] _get_trades_map failed: {exc}")
            return {}

        out: dict[str, dict] = {}
        for t in trades:
            symbol = _from_oanda(t.get("instrument", ""))
            if not symbol:
                continue
            # If a symbol has multiple trades (scaling), prefer the first / keep SL/TP union.
            existing = out.get(symbol)
            sl = float(t.get("stopLossOrder",   {}).get("price", 0) or 0)
            tp = float(t.get("takeProfitOrder", {}).get("price", 0) or 0)
            if existing is None:
                out[symbol] = {
                    "trade_id":    t.get("id", ""),
                    "entry_price": float(t.get("price", 0) or 0),
                    "stop_loss":   sl,
                    "take_profit": tp,
                    "opened_at":   t.get("openTime", ""),
                }
            else:
                # Keep non-zero values from any trade in case the first one is missing them
                if sl and not existing["stop_loss"]:
                    existing["stop_loss"] = sl
                if tp and not existing["take_profit"]:
                    existing["take_profit"] = tp
        return out

    async def get_positions(self) -> list[Position]:
        # Fetch per-trade TP/SL first — openPositions doesn't include them
        trades_map = await self._get_trades_map()

        try:
            resp = await self._http.get(
                f"{self._rest}/accounts/{self._acct}/openPositions"
            )
            resp.raise_for_status()
            raw = resp.json().get("positions", [])
        except Exception as exc:
            logger.error(f"[oanda] get_positions failed: {exc}")
            return []

        out: list[Position] = []
        for p in raw:
            try:
                symbol  = _from_oanda(p["instrument"])
                long_s  = p.get("long",  {})
                short_s = p.get("short", {})
                long_u  = float(long_s.get("units",  "0") or "0")
                short_u = float(short_s.get("units", "0") or "0")

                # Top-level unrealizedPL is already in account currency (USD).
                # Use it in preference to the side-specific value which is also
                # correct but occasionally absent on fresh positions.
                top_pnl = float(p.get("unrealizedPL", 0) or 0)

                trade_info = trades_map.get(symbol, {})
                sl  = trade_info.get("stop_loss",   0.0)
                tp  = trade_info.get("take_profit",  0.0)
                tid = trade_info.get("trade_id",     "")

                if long_u != 0:
                    avg = float(long_s.get("averagePrice", 0) or 0)
                    out.append(Position(
                        symbol            = symbol,
                        direction         = Direction.LONG,
                        quantity          = long_u,
                        entry_price       = avg,
                        current_price     = avg,
                        stop_loss         = sl,
                        take_profit       = tp,
                        broker            = BrokerName.OANDA,
                        broker_order_id   = tid,
                        opened_at         = datetime.now(tz=timezone.utc),
                        unrealized_pnl    = top_pnl if long_u != 0 and short_u == 0
                                            else float(long_s.get("unrealizedPL", 0) or 0),
                    ))
                if short_u != 0:
                    avg = float(short_s.get("averagePrice", 0) or 0)
                    out.append(Position(
                        symbol            = symbol,
                        direction         = Direction.SHORT,
                        quantity          = abs(short_u),
                        entry_price       = avg,
                        current_price     = avg,
                        stop_loss         = sl,
                        take_profit       = tp,
                        broker            = BrokerName.OANDA,
                        broker_order_id   = tid,
                        opened_at         = datetime.now(tz=timezone.utc),
                        unrealized_pnl    = top_pnl if short_u != 0 and long_u == 0
                                            else float(short_s.get("unrealizedPL", 0) or 0),
                    ))
            except Exception as exc:
                logger.warning(f"[oanda] skipping malformed position entry: {exc} | raw={p}")
                continue

        logger.debug(f"[oanda] get_positions → {len(out)} open")
        return out

    async def cancel_order(self, broker_order_id: str) -> bool:
        """
        Cancel a pending order by its OANDA order ID.

        OANDA market orders filled immediately so there is rarely anything to
        cancel, but pending limit / stop orders can be cancelled here.
        """
        try:
            resp = await self._http.put(
                f"{self._rest}/accounts/{self._acct}/orders/{broker_order_id}/cancel"
            )
            resp.raise_for_status()
            logger.info(f"[oanda] cancelled order {broker_order_id}")
            return True
        except Exception as exc:
            logger.error(f"[oanda] cancel_order {broker_order_id} failed: {exc}")
            return False

    # ── Helpers ────────────────────────────────────────────────────────────────

    async def _patch_trade_brackets(
        self,
        trade_id:   str,
        fill_price: float,
        order:      "BracketOrder",
        digits:     int,
    ) -> None:
        """
        PATCH an open trade to attach TP and SL orders.

        TP/SL distances are preserved from the original signal but anchored to
        the *actual fill price* so they are always on the correct side of the
        market — avoiding OANDA's LOSING_TAKE_PROFIT / LOSING_STOP_LOSS errors
        that occur when the signal price is stale relative to the live spread.
        """
        is_long = order.side == OrderSide.BUY

        # Preserve the ATR-based $ distances from the strategy, re-anchor to fill.
        tp_dist = abs(order.take_profit - order.entry_price) if order.take_profit else 0.0
        sl_dist = abs(order.stop_loss  - order.entry_price) if order.stop_loss  else 0.0

        if is_long:
            tp_price = fill_price + tp_dist
            sl_price = fill_price - sl_dist
        else:
            tp_price = fill_price - tp_dist
            sl_price = fill_price + sl_dist

        patch: dict = {}
        if order.take_profit and tp_dist:
            patch["takeProfit"] = {
                "price":       f"{tp_price:.{digits}f}",
                "timeInForce": "GTC",
            }
        if order.stop_loss and sl_dist:
            patch["stopLoss"] = {
                "price":       f"{sl_price:.{digits}f}",
                "timeInForce": "GTC",
            }

        if not patch:
            return

        try:
            resp = await self._http.put(
                f"{self._rest}/accounts/{self._acct}/trades/{trade_id}/orders",
                json=patch,
            )
            resp.raise_for_status()
            logger.info(
                f"[oanda] brackets set | tradeID={trade_id} "
                f"tp={tp_price:.{digits}f} sl={sl_price:.{digits}f}"
            )
        except Exception as exc:
            # Non-fatal: trade is open, just without server-side brackets.
            # The crypto monitor will still manage exits intra-bar.
            logger.warning(f"[oanda] failed to set brackets on {trade_id}: {exc}")

    async def _transaction_stream_loop(self) -> None:
        """
        Consume OANDA's account transaction SSE stream and log TP/SL exits in
        real-time — matching the Alpaca WebSocket fill-event behaviour.

        Reconnects automatically on network errors with exponential back-off.
        The stream is stopped by setting self._tx_stop before disconnect().
        """
        url      = f"{self._stream}/accounts/{self._acct}/transactions/stream"
        headers  = {**self._headers, "Content-Type": "application/octet-stream"}
        delay    = 2.0
        # Only log exits that happen AFTER this stream connection was opened.
        # OANDA may replay recent transactions on connect — ignore anything older.
        connected_at = datetime.now(tz=timezone.utc)

        while not self._tx_stop.is_set():
            try:
                async with httpx.AsyncClient(
                    headers=headers,
                    timeout=httpx.Timeout(connect=10.0, read=None, write=10.0, pool=10.0),
                ) as client:
                    async with client.stream("GET", url) as resp:
                        resp.raise_for_status()
                        logger.info("[oanda] transaction stream connected")
                        delay = 2.0          # reset back-off on successful connect

                        async for raw_line in resp.aiter_lines():
                            if self._tx_stop.is_set():
                                return
                            line = raw_line.strip()
                            if not line:
                                continue
                            try:
                                msg = json.loads(line)
                            except json.JSONDecodeError:
                                continue

                            # Heartbeat — ignore
                            if msg.get("type") == "TRANSACTION_HEARTBEAT":
                                continue

                            if msg.get("type") != "ORDER_FILL":
                                continue

                            # Skip events older than our connect time (OANDA backfill)
                            tx_time_str = msg.get("time", "")
                            if tx_time_str:
                                tx_time = _parse_oanda_dt(tx_time_str)
                                if tx_time and tx_time < connected_at:
                                    continue

                            reason = msg.get("reason", "")
                            if reason not in ("TAKE_PROFIT_ORDER", "STOP_LOSS_ORDER",
                                              "TRAILING_STOP_LOSS_ORDER", "MARKET_ORDER_POSITION_CLOSEOUT",
                                              "MARKET_ORDER_TRADE_CLOSE"):
                                continue

                            instrument = _from_oanda(msg.get("instrument", "?"))
                            units      = float(msg.get("units", 0))
                            price      = float(msg.get("price", 0))
                            pl         = float(msg.get("pl", 0))
                            trade_id   = msg.get("tradesClosed", [{}])[0].get("tradeID", "?") \
                                         if msg.get("tradesClosed") else "?"

                            exit_type = {
                                "TAKE_PROFIT_ORDER":               "TAKE_PROFIT",
                                "STOP_LOSS_ORDER":                 "STOP_LOSS",
                                "TRAILING_STOP_LOSS_ORDER":        "TRAILING_SL",
                                "MARKET_ORDER_POSITION_CLOSEOUT":  "MANUAL_CLOSE",
                                "MARKET_ORDER_TRADE_CLOSE":        "MANUAL_CLOSE",
                            }.get(reason, reason)

                            direction = "LONG" if units < 0 else "SHORT"   # closing units are opposite sign
                            pl_sign   = "+" if pl >= 0 else ""

                            if pl >= 0:
                                logger.success(
                                    f"[oanda] ✓ EXIT {exit_type} | {instrument} {direction} "
                                    f"@ {price} | P&L={pl_sign}{pl:.2f} | tradeID={trade_id}"
                                )
                            else:
                                logger.warning(
                                    f"[oanda] ✗ EXIT {exit_type} | {instrument} {direction} "
                                    f"@ {price} | P&L={pl_sign}{pl:.2f} | tradeID={trade_id}"
                                )

            except asyncio.CancelledError:
                return
            except Exception as exc:
                if self._tx_stop.is_set():
                    return
                logger.warning(
                    f"[oanda] transaction stream disconnected ({exc}), "
                    f"reconnecting in {delay:.0f}s…"
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60.0)

        logger.debug("[oanda] transaction stream stopped")

    async def _fetch_instruments(self) -> None:
        """
        Populate self._tradeable and self._digits_map from
        GET /accounts/{id}/instruments.

        Tradeable set uses KATS internal format ("EURUSD");
        digits map uses OANDA format ("EUR_USD") matching order submission.
        """
        try:
            resp = await self._http.get(
                f"{self._rest}/accounts/{self._acct}/instruments"
            )
            resp.raise_for_status()
            items = resp.json().get("instruments", [])
        except Exception as exc:
            logger.warning(f"[oanda] could not fetch instrument list: {exc}")
            return

        tradeable: set[str]       = set()
        digits:    dict[str, int] = {}

        # Map OANDA instrument type → KATS AssetClass value string.
        # Registered in the global model registry so asset_class("EURUSD")
        # returns AssetClass.FOREX throughout the system (instruments panel,
        # risk gatekeeper, etc.).
        from kronos_trade.models import AssetClass, _SYMBOL_ASSET_CLASS_EXPLICIT
        _TYPE_TO_AC = {
            "CURRENCY": AssetClass.FOREX,
            "METAL":    AssetClass.FOREX,
            "INDEX":    AssetClass.FOREX,    # treat indices as forex-class for routing
            "CFD":      AssetClass.EQUITY,
            "BOND":     AssetClass.EQUITY,
        }

        by_type: dict[str, int] = {}

        for inst in items:
            oanda_name = inst.get("name", "")          # e.g. "EUR_USD"
            precision  = inst.get("displayPrecision", 5)
            inst_type  = inst.get("type", "OTHER")     # "CURRENCY", "CFD", "METAL"…

            if not oanda_name:
                continue

            kats_name  = _from_oanda(oanda_name)        # "EURUSD"
            margin_str = inst.get("marginRate", "0.0333")
            tradeable.add(kats_name)
            digits[oanda_name]        = precision
            self._margin_rates[oanda_name] = float(margin_str or "0.0333")
            by_type[inst_type] = by_type.get(inst_type, 0) + 1

            # Register into global asset-class map so downstream code can
            # identify the instrument without a separate lookup.
            ac = _TYPE_TO_AC.get(inst_type)
            if ac and kats_name not in _SYMBOL_ASSET_CLASS_EXPLICIT:
                _SYMBOL_ASSET_CLASS_EXPLICIT[kats_name] = ac

        self._tradeable  = tradeable
        self._digits_map = digits

        summary = ", ".join(f"{v} {k}" for k, v in sorted(by_type.items()))
        logger.info(
            f"[oanda] {len(tradeable)} tradeable instruments loaded "
            f"({summary})"
        )

    def _instrument_digits(self, instrument: str) -> int:
        """
        Return cached decimal precision for an instrument (e.g. "EUR_USD" → 5).
        Falls back to 5 if the instrument wasn't in the connect-time fetch
        (e.g. exotic pair added after startup).
        """
        return self._digits_map.get(instrument, 5)

    async def sync_history(self, store, days_back: int = 90) -> int:
        """
        Pull closed trades from OANDA and upsert into TradeStore.
        Uses GET /v3/accounts/{id}/trades?state=CLOSED which returns each
        completed trade with averageClosePrice, openTime, closeTime, realizedPL.
        Returns number of new records inserted.
        """
        count = 0
        last_id: str | None = None

        while True:
            params: dict = {"state": "CLOSED", "count": "500"}
            if last_id:
                params["beforeID"] = last_id
            else:
                # Limit initial fetch to days_back; use Z suffix (OANDA prefers it)
                from_dt = datetime.now(tz=timezone.utc) - timedelta(days=days_back)
                params["from"] = from_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

            try:
                resp = await self._http.get(
                    f"{self._rest}/accounts/{self._acct}/trades",
                    params=params,
                )
                resp.raise_for_status()
                trades = resp.json().get("trades", [])
            except Exception as exc:
                logger.error(f"[oanda] sync_history fetch failed: {exc}")
                break

            if not trades:
                break

            for t in trades:
                try:
                    symbol = _from_oanda(t.get("instrument", ""))
                    if not symbol:
                        continue

                    units       = float(t.get("initialUnits", 0))
                    direction   = "long" if units > 0 else "short"
                    entry_price = float(t.get("price", 0))
                    exit_price  = float(t.get("averageClosePrice", 0) or 0)
                    realized    = float(t.get("realizedPL", 0))

                    # OANDA timestamps have nanosecond precision — use helper that
                    # truncates to microseconds before parsing.
                    open_dt  = _parse_oanda_dt(t.get("openTime",  ""))
                    close_dt = _parse_oanda_dt(t.get("closeTime", ""))

                    if not open_dt or not close_dt:
                        logger.warning(f"[oanda] sync_history trade {t.get('id')}: missing timestamps")
                        continue

                    sl_price = float(t.get("stopLossOrder",   {}).get("price", 0) or 0) or None
                    tp_price = float(t.get("takeProfitOrder", {}).get("price", 0) or 0) or None

                    # Infer exit reason from SL/TP orders and P&L sign
                    if realized > 0 and tp_price:
                        exit_reason = "tp"
                    elif realized < 0 and sl_price:
                        exit_reason = "sl"
                    else:
                        exit_reason = "manual"

                    duration = int((close_dt - open_dt).total_seconds())
                    r_risked = abs(entry_price - sl_price) if sl_price else None
                    qty      = abs(units)
                    rr       = realized / (r_risked * qty) if r_risked and r_risked > 0 and qty else None

                    broker_key = f"oanda_{'practice' if settings.oanda_practice else 'live'}"
                    inserted = await store.upsert_trade_from_history(
                        broker_order_id  = t["id"],
                        symbol           = symbol,
                        direction        = direction,
                        quantity         = qty,
                        broker           = broker_key,
                        entry_price      = entry_price,
                        exit_price       = exit_price,
                        planned_sl       = sl_price,
                        planned_tp       = tp_price,
                        entry_datetime   = open_dt,
                        exit_datetime    = close_dt,
                        exit_reason      = exit_reason,
                        realized_pnl     = realized,
                        duration_seconds = duration,
                        r_risked         = r_risked,
                        rr_achieved      = rr,
                        is_winner        = realized > 0,
                    )
                    if inserted:
                        count += 1
                except Exception as exc:
                    logger.warning(f"[oanda] sync_history trade {t.get('id')}: {exc}")

            # Paginate: if fewer than 500 returned, we're done
            if len(trades) < 500:
                break
            last_id = trades[-1]["id"]

        logger.info(f"[oanda] sync_history complete: {count} new trades inserted")
        return count

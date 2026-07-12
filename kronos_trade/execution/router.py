"""
kronos_trade/execution/router.py
Execution router — the central trading loop.

Subscribes to the data pipeline, triggers Kronos on each new bar,
passes signals through strategy + risk, and dispatches orders to brokers.
"""
from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from datetime import datetime, timezone
import time as _time
from pathlib import Path

from loguru import logger

from kronos_trade.config import TradingMode, kats_cfg, settings
from kronos_trade.data.pipeline import DataPipeline
from kronos_trade.kronos.predictor import KronosEngine
from kronos_trade.kronos.signals import SignalGenerator
from kronos_trade.models import (
    AccountSnapshot, BracketOrder, BrokerName, Direction, EntryDetail,
    KronosSignal, OHLCVBar, Position, SystemState, OrderStatus,
    AssetClass, asset_class as get_asset_class,
)
from kronos_trade.strategy.engine import StrategyEngine
from kronos_trade.strategy.risk import RiskGatekeeper

from .brokers.base import BrokerAdapter

_PARAMS_FILE  = Path("kronos_position_params.json")
_STATE_FILE   = Path("kronos_router_state.json")


def _compound_broker_key(broker: BrokerName) -> str:
    """Return a compound broker+env key for DB isolation (mirrors run_system.py and oanda.py)."""
    if broker == BrokerName.OANDA:
        return f"oanda_{'practice' if settings.oanda_practice else 'live'}"
    if broker == BrokerName.ALPACA:
        return f"alpaca_{'paper' if settings.alpaca_paper else 'live'}"
    return broker.value


def _sanitize_entry(e: dict) -> dict:
    """
    Coerce a raw entry dict (possibly loaded from an old params file) into
    the types expected by EntryDetail.
    - submitted_at: convert ISO-string → Unix float; leave floats unchanged
    - tp_order_id / sl_order_id: convert None → ""
    """
    out = dict(e)
    sat = out.get("submitted_at", 0)
    if isinstance(sat, str):
        try:
            from datetime import datetime, timezone
            out["submitted_at"] = datetime.fromisoformat(sat).timestamp()
        except Exception:
            out["submitted_at"] = 0.0
    out["tp_order_id"] = out.get("tp_order_id") or ""
    out["sl_order_id"] = out.get("sl_order_id") or ""
    return out


class ExecutionRouter:
    """
    Orchestrates the full pipeline from bar → signal → order → broker.

    Usage:
        router = ExecutionRouter(pipeline, brokers=[AlpacaAdapter()])
        await router.start()
        # ... runs until stopped
        await router.stop()
    """

    def __init__(
        self,
        pipeline: DataPipeline,
        brokers: list[BrokerAdapter],
        symbols: list[str] | None = None,
        timeframe: str | None = None,
        primary_broker: BrokerName = BrokerName.ALPACA,
        price_feed=None,
        store=None,
    ) -> None:
        self.pipeline        = pipeline
        self.brokers         = {b.name: b for b in brokers}
        self.symbols         = symbols or kats_cfg.instrument_list
        self.timeframe       = timeframe or kats_cfg.default_timeframe
        self.primary_broker  = primary_broker

        self.kronos   = KronosEngine()
        self.signals  = SignalGenerator(self.kronos, pipeline)
        self.strategy = StrategyEngine()
        self.risk     = RiskGatekeeper()

        self._bar_queue:  asyncio.Queue[OHLCVBar] = asyncio.Queue()
        self._positions:  list[Position]           = []
        self._account:    AccountSnapshot | None   = None
        self._state       = SystemState()
        self._tasks:      list[asyncio.Task]       = []
        self._running     = False
        self._start_time: datetime | None          = None

        # Per-symbol: last bar we ran inference on (avoid duplicate runs)
        self._last_bar_ts: dict[str, datetime] = {}

        # Broadcast queue for the dashboard / websocket
        self._broadcast_queues: list[asyncio.Queue] = []

        self._price_feed = price_feed
        self._store = store   # TradeStore | None — for journal persistence

        # dict[symbol, list[entry_dict]]  — supports multiple entries per symbol
        self._position_params: dict[str, list[dict]] = {}

        self._submitted_at: dict[str, float] = {}
        self._FILL_GRACE = 90.0

        # Mutable trading mode — can be switched at runtime via API
        self.trading_mode: TradingMode = kats_cfg.trading_mode

        # Kronos inference pause — skips signal generation without stopping system
        self.kronos_paused: bool = False

        self._load_params()
        self._load_state()

    # ── SL/TP persistence ─────────────────────────────────────────────────────

    def _save_params(self) -> None:
        try:
            serializable = {}
            for sym, entries in self._position_params.items():
                serializable[sym] = [
                    {k: (v.value if hasattr(v, "value") else v) for k, v in e.items()}
                    for e in entries
                ]
            _PARAMS_FILE.write_text(json.dumps(serializable, indent=2))
        except Exception as exc:
            logger.debug(f"[router] save params: {exc}")

    def _save_state(self) -> None:
        """Persist runtime flags. trading_mode/timeframe are in kats_config.json."""
        from datetime import date as _date
        try:
            rs = self.risk.state
            _STATE_FILE.write_text(json.dumps({
                "kronos_paused":      self.kronos_paused,
                "kill_switch":        rs.kill_switch,
                "daily_halted":       rs.daily_halted,
                "daily_halt_date":    rs.daily_date.isoformat(),
                "drawdown_halted":    rs.drawdown_halted,
                # saved to detect if operator raised the limit to clear DD halt
                "_dd_halt_threshold": kats_cfg.max_drawdown_pct,
            }, indent=2))
        except Exception as exc:
            logger.debug(f"[router] save state: {exc}")

    def _load_state(self) -> None:
        """Restore persisted runtime flags.
        trading_mode and timeframe come from kats_config.json via kats_cfg."""
        try:
            if not _STATE_FILE.exists():
                return
            data = json.loads(_STATE_FILE.read_text())

            if "kronos_paused" in data:
                self.kronos_paused = bool(data["kronos_paused"])
            if data.get("kill_switch"):
                self.risk.engage_kill_switch()

            # daily halt — restore only if set today
            if data.get("daily_halted"):
                from datetime import date as _date
                halt_date_str = data.get("daily_halt_date", "")
                try:
                    halt_date = _date.fromisoformat(halt_date_str)
                except ValueError:
                    halt_date = None
                if halt_date and halt_date == _date.today():
                    self.risk.state.daily_halted = True
                    logger.warning("[router] daily loss halt restored from previous session")
                else:
                    logger.info("[router] daily halt was from a prior day — cleared")

            # drawdown halt — persists until max_drawdown_pct is raised in kats_config.json
            if data.get("drawdown_halted"):
                saved_threshold = data.get("_dd_halt_threshold", kats_cfg.max_drawdown_pct)
                if kats_cfg.max_drawdown_pct > saved_threshold:
                    logger.info(
                        f"[router] max_drawdown_pct raised "
                        f"({saved_threshold}% → {kats_cfg.max_drawdown_pct}%) — DD halt cleared"
                    )
                else:
                    self.risk.state.drawdown_halted = True
                    logger.warning(
                        "[router] drawdown halt restored — raise max_drawdown_pct in "
                        "kats_config.json to clear"
                    )

            logger.info(
                f"[router] restored state: mode={self.trading_mode.value} "
                f"tf={self.timeframe} "
                f"paused={self.kronos_paused} "
                f"kill_switch={self.risk.state.kill_switch} "
                f"daily_halted={self.risk.state.daily_halted} "
                f"drawdown_halted={self.risk.state.drawdown_halted}"
            )
            # Migrate: remove stale fields that moved to kats_config.json
            _stale = {"trading_mode", "timeframe", "_env_trading_mode", "_env_timeframe"}
            if _stale & data.keys():
                for _k in _stale:
                    data.pop(_k, None)
                _STATE_FILE.write_text(json.dumps(data, indent=2))
                logger.info("[router] state file migrated — removed stale fields")
        except Exception as exc:
            logger.debug(f"[router] load state: {exc}")

    def _load_params(self) -> None:
        """Restore _position_params from disk on startup."""
        try:
            if not _PARAMS_FILE.exists():
                return
            data = json.loads(_PARAMS_FILE.read_text())
            if not data:
                return
            for sym, val in data.items():
                # Migrate old single-dict format to list
                self._position_params[sym] = [val] if isinstance(val, dict) else val

            # Seed _submitted_at for every restored symbol so the cleanup loop
            # treats them as "just submitted" during the startup grace window.
            # Without this, submitted_at defaults to 0 → now-0 is epoch-scale →
            # exceeds _FILL_GRACE instantly → all params wiped before broker
            # positions are confirmed on the first _refresh_positions() call.
            startup_ts = _time.time()
            for sym in self._position_params:
                self._submitted_at[sym] = startup_ts

            logger.info(
                f"[router] restored position params for: {list(data.keys())} "
                f"(grace window {self._FILL_GRACE:.0f}s)"
            )
        except Exception as exc:
            logger.debug(f"[router] load params: {exc}")

    # ── Public API ────────────────────────────────────────────────────────────

    async def start(self) -> None:
        if self._running:
            return
        self._running   = True
        self._start_time = datetime.now(tz=timezone.utc)

        # Parse trading schedule (if configured)
        from kronos_trade.utils.schedule import TradingSchedule
        self._schedule: TradingSchedule | None = None
        if kats_cfg.trading_schedule:
            try:
                self._schedule = TradingSchedule(kats_cfg.trading_schedule)
                logger.info(f"[router] trading schedule: {self._schedule}")
            except ValueError as exc:
                logger.warning(f"[router] ignoring invalid trading_schedule: {exc}")

        logger.info("[router] loading Kronos model…")
        await self.kronos.load()
        self._state.kronos_loaded = True

        logger.info("[router] connecting brokers…")
        for broker in self.brokers.values():
            await broker.connect()

        # Seed risk gatekeeper with initial equity
        await self._refresh_account()
        if self._account:
            self.risk.initialize(self._account.equity)

        # Populate positions immediately so restored _position_params are
        # validated against the broker before any background tasks run.
        # This prevents the cleanup loop from wiping params during the 30s
        # window before _position_loop first wakes up.
        await self._refresh_positions()

        # Subscribe to the pipeline bar stream
        self._bar_queue = self.pipeline.subscribe()

        # Start background tasks
        self._tasks = [
            asyncio.create_task(self._bar_loop(),                name="bar-loop"),
            asyncio.create_task(self._account_loop(),            name="account-loop"),
            asyncio.create_task(self._position_loop(),           name="position-loop"),
            asyncio.create_task(self._crypto_monitor_loop(),     name="crypto-monitor"),
            asyncio.create_task(self._exit_order_monitor_loop(), name="exit-order-monitor"),
        ]
        self._state.running = True
        logger.success("[router] ✓ execution router running")

        # Broadcast restored state so dashboards pick it up immediately
        await self._broadcast({"type": "mode_switch",       "data": {"mode":      self.trading_mode.value}})
        await self._broadcast({"type": "kronos_pause",      "data": {"paused":    self.kronos_paused}})
        await self._broadcast({"type": "kill_switch",       "data": {"active":    self.risk.state.kill_switch}})
        await self._broadcast({"type": "timeframe_switch",  "data": {"timeframe": self.timeframe}})
        await self._broadcast({"type": "risk_halt",         "data": {
            "daily_halted":    self.risk.state.daily_halted,
            "drawdown_halted": self.risk.state.drawdown_halted,
        }})


    async def stop(self) -> None:
        self._running = False
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        for broker in self.brokers.values():
            await broker.disconnect()
        self._state.running = False
        logger.info("[router] stopped")

    def subscribe_broadcasts(self) -> asyncio.Queue:
        """Get a queue that receives every signal/order/state event dict."""
        q: asyncio.Queue = asyncio.Queue(maxsize=200)
        self._broadcast_queues.append(q)
        return q

    @property
    def state(self) -> SystemState:
        elapsed = (
            (datetime.now(tz=timezone.utc) - self._start_time).total_seconds()
            if self._start_time else 0.0
        )
        self._state.uptime_seconds    = elapsed
        self._state.positions         = list(self._positions)
        self._state.kill_switch       = self.risk.state.kill_switch
        self._state.kronos_paused     = self.kronos_paused
        self._state.daily_loss_halted = self.risk.state.daily_halted
        self._state.drawdown_halted   = self.risk.state.drawdown_halted
        self._state.trading_mode      = self.trading_mode.value
        self._state.timeframe         = self.timeframe
        if self._account:
            self._state.daily_pnl = self._account.daily_pnl
        return self._state

    # ── Background loops ──────────────────────────────────────────────────────

    async def _bar_loop(self) -> None:
        """
        Main trading loop: consume bars from pipeline, run Kronos, submit orders.
        Respects trading_schedule: if outside the configured window, bars are
        drained and discarded while a STANDBY broadcast is sent every 30 s.
        """
        _in_standby = False

        while self._running:
            # ── Schedule gate ────────────────────────────────────────────────
            if self._schedule and not self._schedule.is_active():
                if not _in_standby:
                    _in_standby = True
                    logger.info("[router] outside trading window — entering standby")

                nxt  = self._schedule.next_open()
                diff = nxt - datetime.now(tz=timezone.utc)
                h, rem = divmod(int(diff.total_seconds()), 3600)
                m = rem // 60
                await self._broadcast({
                    "type":      "standby",
                    "next_open": nxt.strftime("%H:%M UTC"),
                    "countdown": f"{h}h {m}m",
                })
                # Drain any queued bars so they don't pile up during standby
                try:
                    while True:
                        self._bar_queue.get_nowait()
                except Exception:
                    pass
                await asyncio.sleep(30)
                continue

            # ── Leaving standby ──────────────────────────────────────────────
            if _in_standby:
                _in_standby = False
                logger.info("[router] trading window opened — resuming")
                await self._broadcast({"type": "standby", "next_open": None, "countdown": ""})

            try:
                bar: OHLCVBar = await asyncio.wait_for(
                    self._bar_queue.get(), timeout=2.0
                )
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break

            if bar.symbol not in self.symbols:
                continue

            # Debounce: only run inference once per bar timestamp per symbol
            last = self._last_bar_ts.get(bar.symbol)
            if last and bar.timestamp <= last:
                continue
            self._last_bar_ts[bar.symbol] = bar.timestamp

            try:
                await self._process_bar(bar)
            except Exception as exc:
                logger.error(f"[router] process_bar error for {bar.symbol}: {exc}")

    async def _process_bar(self, bar: OHLCVBar) -> None:
        if self.kronos_paused:
            return

        signal = await self.signals.generate(bar.symbol, self.timeframe)
        if signal is None:
            return

        self._state.last_signal = signal
        await self._broadcast({"type": "signal", "data": signal.model_dump(mode="json")})
        if self._store:
            asyncio.create_task(self._store.save_signal(signal))

        # Get current account state
        account = self._account
        if not account:
            return

        # Build order from signal
        order = self.strategy.build_order(
            signal,
            account_equity=account.equity,
            broker=self.primary_broker,
            trading_mode=self.trading_mode,
        )
        if order is None:
            return

        # Risk check
        approved, reason = self.risk.check_order(
            order, self._positions, account.equity,
            confidence=signal.confidence,
            trading_mode=self.trading_mode,
        )
        if not approved:
            await self._broadcast({"type": "risk_reject", "data": {"reason": reason}})
            return

        # Submit to broker
        broker = self.brokers.get(self.primary_broker)
        if not broker:
            logger.error(f"[router] broker {self.primary_broker} not connected")
            return

        filled_order = await broker.submit_bracket_order(order)

        if filled_order.status == OrderStatus.REJECTED:
            reason = filled_order.reject_reason or f"Broker rejected order for {order.symbol}"
            await self._broadcast({"type": "risk_reject", "data": {"reason": reason}})
            return

        if filled_order.status == OrderStatus.SUBMITTED:
            direction_val = signal.direction.value if hasattr(signal.direction, "value") else signal.direction
            new_entry = {
                "order_id":    filled_order.broker_order_id or "",
                "direction":   direction_val,
                "quantity":    order.quantity,
                "entry_price": order.entry_price,
                "stop_loss":   order.stop_loss,
                "take_profit": order.take_profit,
                "submitted_at": _time.time(),
                "tp_order_id": "",
                "sl_order_id": "",
            }
            self._position_params.setdefault(order.symbol, []).append(new_entry)
            self._submitted_at[order.symbol] = _time.time()
            self._save_params()
            logger.info(
                f"[router] stored entry #{len(self._position_params[order.symbol])} "
                f"for {order.symbol} sl={order.stop_loss:.4f} tp={order.take_profit:.4f}"
            )

            # For crypto, submit broker-side TP/SL exit orders after entry fills
            if get_asset_class(order.symbol) == AssetClass.CRYPTO and filled_order.broker_order_id:
                asyncio.create_task(
                    self._monitor_and_place_exits(
                        symbol=order.symbol,
                        entry_order_id=filled_order.broker_order_id,
                        qty=order.quantity,
                        direction=direction_val,
                        sl_price=order.stop_loss,
                        tp_price=order.take_profit,
                    )
                )

        await self._broadcast({
            "type": "order",
            "data": filled_order.model_dump(mode="json"),
        })

        # Persist to journal DB (use compound key so live/demo are stored separately)
        if self._store and filled_order.status == OrderStatus.SUBMITTED:
            asyncio.create_task(self._store.save_trade_open(
                filled_order,
                signal_confidence=signal.confidence,
                timeframe=self.timeframe,
                broker_key=_compound_broker_key(self.primary_broker),
            ))

        asyncio.create_task(self._delayed_refresh(0.5))

    async def _delayed_refresh(self, delay: float) -> None:
        await asyncio.sleep(delay)
        await self._refresh_positions()

    async def _monitor_and_place_exits(
        self,
        symbol: str,
        entry_order_id: str,
        qty: float,
        direction: str,
        sl_price: float,
        tp_price: float,
    ) -> None:
        """
        Wait for the crypto market entry to fill, then submit broker-side
        TP (limit) and SL (stop-limit) exit orders.
        Falls back to price monitor if the fill isn't detected within 60s.
        """
        broker = self.brokers.get(self.primary_broker)
        if not broker or not hasattr(broker, "get_order_fill"):
            return

        filled_qty   = qty
        filled_price = 0.0

        for attempt in range(30):        # poll up to 60s (2s intervals)
            await asyncio.sleep(2)
            result = await broker.get_order_fill(entry_order_id)
            if result:
                filled_qty, filled_price = result
                break
        else:
            logger.warning(
                f"[router] {symbol}: entry {entry_order_id} not confirmed filled after 60s "
                "— broker-side exits skipped, using price monitor"
            )
            return

        logger.info(
            f"[router] {symbol}: entry filled qty={filled_qty} @ {filled_price:.4f} "
            "— placing broker-side TP/SL"
        )

        try:
            tp_id, sl_id = await broker.submit_exit_orders(
                symbol=symbol,
                qty=filled_qty,
                direction=direction,
                sl_price=sl_price,
                tp_price=tp_price,
            )
        except Exception as exc:
            logger.warning(
                f"[router] {symbol}: broker-side exit placement failed ({exc}) "
                "— falling back to price monitor"
            )
            return

        # Update the matching entry in _position_params with exit order IDs + actual fill
        entries = self._position_params.get(symbol, [])
        for entry in entries:
            if entry.get("order_id") == entry_order_id:
                entry["tp_order_id"]  = tp_id
                entry["sl_order_id"]  = sl_id
                entry["entry_price"]  = filled_price if filled_price else entry["entry_price"]
                entry["quantity"]     = filled_qty
                break
        self._save_params()
        logger.info(f"[router] {symbol}: broker exits set tp={tp_id[:8]}… sl={sl_id[:8]}…")

    async def _account_loop(self) -> None:
        while self._running:
            try:
                # Refresh account every 30s
                await asyncio.sleep(30)
                await self._refresh_account()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error(f"[router] account_loop error: {exc}")

    async def _position_loop(self) -> None:
        """Faster refresh when positions are open — every 5s vs 30s."""
        while self._running:
            try:
                interval = 5 if self._positions else 30
                await asyncio.sleep(interval)
                await self._refresh_positions()

                if self._price_feed:
                    # Ticker shows configured symbols PLUS any symbol with an open
                    # position so the display stays complete regardless of config.
                    position_syms = {p.symbol for p in self._positions}
                    all_syms = set(self.symbols) | position_syms

                    # Dynamically expand the price feed's poll list for any new
                    # position symbols that weren't in the original instrument list.
                    for sym in position_syms:
                        if sym not in self._price_feed.symbols:
                            self._price_feed.symbols.append(sym)
                            logger.info(f"[router] added {sym} to price feed (open position)")

                    prices = {
                        sym: self._price_feed.latest(sym)
                        for sym in all_syms
                        if self._price_feed.latest(sym) is not None
                    }

                    if prices: await self._broadcast({"type": "prices", "data": prices})
            except asyncio.CancelledError: break
            except Exception as exc:
                logger.error(f"[router] position_loop error: {exc}")
    async def _refresh_account(self) -> None:
        broker = self.brokers.get(self.primary_broker)
        if not broker: return
        try:
            new_account = await broker.get_account()
            if new_account.equity > 0:
                self._account = new_account
                # Snapshot halt flags before update so we can detect transitions
                _was_daily_halted = self.risk.state.daily_halted
                _was_dd_halted    = self.risk.state.drawdown_halted
                self.risk.update_equity(self._account.equity)
                await self._broadcast({
                    "type": "account",
                    "data": self._account.model_dump(mode="json"),
                })
                # Persist and broadcast any newly triggered halts
                if (self.risk.state.daily_halted  != _was_daily_halted or
                        self.risk.state.drawdown_halted != _was_dd_halted):
                    self._save_state()
                    await self._broadcast({
                        "type": "risk_halt",
                        "data": {
                            "daily_halted":     self.risk.state.daily_halted,
                            "drawdown_halted":  self.risk.state.drawdown_halted,
                        }
                    })
        except Exception as exc:
            logger.error(f"[router] refresh_account error: {exc}")

    async def _refresh_positions(self) -> None:
        broker = self.brokers.get(self.primary_broker)
        if not broker:
            logger.warning(f"[router] _refresh_positions: no broker for {self.primary_broker}")
            return
        try:
            positions = await broker.get_positions()
            if positions:
                logger.debug(
                    f"[router] broker returned {len(positions)} open position(s): "
                    + ", ".join(f"{p.symbol}({p.direction.value})" for p in positions)
                )
            elif self._positions:
                # Had positions last cycle, now none — log the transition clearly
                logger.info("[router] all positions closed (broker reports 0 open)")

            needs_save = False
            for pos in positions:
                entries = self._position_params.get(pos.symbol, [])
                if entries:
                    if len(entries) == 1:
                        # Single entry: trust broker's actual fill price & qty
                        entries[0]["entry_price"] = pos.entry_price
                        entries[0]["quantity"]    = pos.quantity
                    else:
                        # Multi-entry: broker owns the authoritative total quantity.
                        # Normalise each entry's stored qty proportionally so they
                        # always sum to pos.quantity — corrects pre-fill planned
                        # quantities that may differ from actual fills.
                        stored_sum = sum(e.get("quantity", 0.0) for e in entries)
                        if stored_sum > 0 and abs(stored_sum - pos.quantity) / stored_sum > 0.001:
                            scale = pos.quantity / stored_sum
                            # Build adjusted copies for display; don't mutate stored params
                            entries = [
                                {**e, "quantity": round(e.get("quantity", 0.0) * scale, 8)}
                                for e in entries
                            ]
                    pos.stop_loss   = entries[0].get("stop_loss",  0.0)
                    pos.take_profit = entries[0].get("take_profit", 0.0)
                else:
                    # Position exists on the broker but has no local params entry —
                    # e.g. KATS was restarted while a trade was open.  Adopt it so
                    # TP/SL are tracked, the TUI shows correct values, and the params
                    # file is saved for the next restart.
                    synthetic = {
                        "order_id":     pos.broker_order_id or f"adopted-{pos.symbol}",
                        "direction":    pos.direction.value,
                        "quantity":     pos.quantity,
                        "entry_price":  pos.entry_price,
                        "stop_loss":    pos.stop_loss,
                        "take_profit":  pos.take_profit,
                        "submitted_at": (
                            pos.opened_at.timestamp()
                            if pos.opened_at
                            else datetime.now(tz=timezone.utc).timestamp()
                        ),
                        "tp_order_id":  "",
                        "sl_order_id":  "",
                    }
                    self._position_params[pos.symbol] = [synthetic]
                    self._submitted_at[pos.symbol]    = datetime.now(tz=timezone.utc)
                    needs_save = True
                    logger.info(
                        f"[router] adopted pre-existing {pos.symbol} "
                        f"({pos.direction.value}) position "
                        f"entry={pos.entry_price} sl={pos.stop_loss} tp={pos.take_profit}"
                    )
                pos.entries = [EntryDetail(**_sanitize_entry(e)) for e in entries]
                if self._price_feed:
                    current = self._price_feed.latest(pos.symbol)
                    if current:
                        if self.primary_broker == BrokerName.OANDA:
                            # OANDA already provides unrealizedPL in account
                            # currency (USD) via the API — don't overwrite it
                            # with (price − entry) × units which gives the
                            # wrong currency for non-USD quote pairs (JPY, GBP…).
                            # Just update current_price for display.
                            pos.current_price = current
                        else:
                            pos.update_pnl(current)

            self._positions = positions
            if needs_save:
                self._save_params()
            open_syms = {p.symbol for p in self._positions}

            now = _time.time()
            for sym in list(self._position_params.keys()):
                if sym in open_syms:
                    self._submitted_at.pop(sym, None)
                    continue

                submitted = self._submitted_at.get(sym, 0)
                if now - submitted < self._FILL_GRACE:
                    logger.debug(
                        f"[router] {sym}: awaiting fill confirmation "
                        f"({now - submitted:.1f}s) < {self._FILL_GRACE}s grace"
                    )
                    continue

                logger.info(f"[router] position closed: {sym} (all entries cleared)")
                entries = self._position_params.pop(sym, None) or []
                self._submitted_at.pop(sym, None)

                # Record exit in journal using last-known position state
                if self._store:
                    last_pos = next((p for p in self._positions if p.symbol == sym), None)
                    if last_pos:
                        exit_price  = last_pos.current_price or last_pos.entry_price
                        realized    = last_pos.unrealized_pnl
                        order_id    = last_pos.broker_order_id or (entries[0].get("order_id") if entries else "")
                        asyncio.create_task(self._store.close_open_trades_for_symbol(
                            symbol=sym,
                            exit_price=exit_price,
                            realized_pnl=realized,
                            exit_reason="unknown",
                        ))

                # Cancel any orphan broker-side TP/SL orders (one filled, cancel the other)
                has_exits = any(e.get("tp_order_id") or e.get("sl_order_id") for e in entries)
                if has_exits and broker and hasattr(broker, "cancel_orders_for_symbol"):
                    asyncio.create_task(broker.cancel_orders_for_symbol(sym))
                self._save_params()

            await self._broadcast({
                "type": "positions",
                "data": [p.model_dump(mode="json") for p in self._positions]
            })

        except Exception as exc:
            logger.error(f"[router] refresh_positions error: {exc}")
            # Still broadcast last-known positions so TUI stays consistent
            await self._broadcast({
                "type": "positions",
                "data": [p.model_dump(mode="json") for p in self._positions],
            })

    async def _crypto_monitor_loop(self) -> None:
        """
        Fallback SL/TP monitor for crypto entries that don't have broker-side exit orders.
        Entries with tp_order_id/sl_order_id set are handled by the broker; skip them.
        """
        from kronos_trade.models import Direction

        while self._running:
            try:
                # Only poll actively when there are unmanaged crypto positions.
                # When trading forex/equities with broker-side brackets the price
                # monitor is completely idle — no need to spin every 10s.
                has_unmanaged_crypto = any(
                    get_asset_class(p.symbol) == AssetClass.CRYPTO and
                    not all(
                        e.get("tp_order_id") or e.get("sl_order_id")
                        for e in self._position_params.get(p.symbol, [{"_": 1}])
                    )
                    for p in self._positions
                )
                await asyncio.sleep(5 if has_unmanaged_crypto else 60)

                if not self._price_feed: continue

                for pos in list(self._positions):
                    if get_asset_class(pos.symbol) != AssetClass.CRYPTO: continue

                    entries = self._position_params.get(pos.symbol, [])

                    # Skip if ALL entries already have broker-side exits placed
                    if entries and all(
                        e.get("tp_order_id") or e.get("sl_order_id") for e in entries
                    ):
                        continue

                    # Only monitor entries without broker-side exits
                    unmanaged = [
                        e for e in entries
                        if not e.get("tp_order_id") and not e.get("sl_order_id")
                    ] if entries else [{"stop_loss": pos.stop_loss, "take_profit": pos.take_profit,
                                        "quantity": pos.quantity}]

                    current = self._price_feed.latest(pos.symbol)
                    if not current: continue

                    broker = self.brokers.get(self.primary_broker)
                    if not broker: continue

                    # Evaluate each entry independently so a TP/SL hit on one
                    # entry only closes that entry's quantity, not the whole position.
                    any_closed = False
                    for entry in list(unmanaged):
                        # Skip if this entry was removed by another loop concurrently
                        current_entries = self._position_params.get(pos.symbol, [])
                        if entry not in current_entries and current_entries:
                            continue
                        sl  = entry.get("stop_loss",  0.0)
                        tp  = entry.get("take_profit", 0.0)
                        qty = entry.get("quantity", 0.0)
                        if not sl and not tp: continue

                        hit_tp = hit_sl = False
                        if pos.direction == Direction.LONG:
                            hit_tp = tp > 0 and current >= tp
                            hit_sl = sl > 0 and current <= sl
                        else:
                            hit_tp = tp > 0 and current <= tp
                            hit_sl = sl > 0 and current >= sl

                        if not hit_tp and not hit_sl:
                            continue

                        reason = "take_profit" if hit_tp else "stop_loss"
                        logger.info(
                            f"[router] price-monitor {reason} hit | "
                            f"{pos.symbol} entry qty={qty} @ {current:.4f}"
                        )

                        # Partial close for this entry's quantity only
                        if qty and hasattr(broker, "close_position_qty"):
                            success = await broker.close_position_qty(pos.symbol, qty)
                        else:
                            # Fallback: close entire position (single-entry case)
                            success = await broker.close_position(pos.symbol)

                        if success:
                            # Remove only this entry from params
                            sym_entries = self._position_params.get(pos.symbol, [])
                            try:
                                sym_entries.remove(entry)
                            except ValueError:
                                pass
                            if not sym_entries:
                                self._position_params.pop(pos.symbol, None)
                            else:
                                self._position_params[pos.symbol] = sym_entries
                            self._save_params()
                            any_closed = True

                        await self._broadcast({
                            "type": "exit",
                            "data": {"symbol": pos.symbol, "reason": reason, "price": current}
                        })

                    if any_closed:
                        await self._refresh_positions()

            except asyncio.CancelledError: break
            except Exception as exc: logger.error(f"[router] crypto_monitor error: {exc}")

    async def _exit_order_monitor_loop(self) -> None:
        """
        Watches broker-side TP/SL order pairs stored in _position_params.
        When one of the pair fills, cancels the companion and removes the
        entry from params so its orphaned order cannot accidentally close
        another entry's position.
        """
        while self._running:
            try:
                await asyncio.sleep(30)

                broker = self.brokers.get(self.primary_broker)
                if not broker or not hasattr(broker, "get_order_fill"):
                    continue
                if not hasattr(broker, "cancel_order"):
                    continue

                refresh_needed = False
                for sym, entries in list(self._position_params.items()):
                    for entry in list(entries):
                        tp_id = entry.get("tp_order_id", "")
                        sl_id = entry.get("sl_order_id", "")
                        if not tp_id and not sl_id:
                            continue  # unmanaged — handled by price monitor

                        tp_filled = sl_filled = False
                        if tp_id:
                            result = await broker.get_order_fill(tp_id)
                            tp_filled = result is not None
                        if sl_id and not tp_filled:
                            result = await broker.get_order_fill(sl_id)
                            sl_filled = result is not None

                        if not tp_filled and not sl_filled:
                            continue

                        # One exit fired — cancel the companion order
                        companion_id = sl_id if tp_filled else tp_id
                        reason       = "take_profit" if tp_filled else "stop_loss"
                        if companion_id:
                            try:
                                await broker.cancel_order(companion_id)
                                logger.info(
                                    f"[router] {sym} {reason} filled — "
                                    f"cancelled companion order {companion_id[-8:]}"
                                )
                            except Exception as e:
                                logger.debug(
                                    f"[router] companion cancel {companion_id[-8:]}: {e}"
                                )

                        # Remove this entry from params
                        sym_entries = self._position_params.get(sym, [])
                        try:
                            sym_entries.remove(entry)
                        except ValueError:
                            pass
                        if not sym_entries:
                            self._position_params.pop(sym, None)
                        else:
                            self._position_params[sym] = sym_entries
                        self._save_params()
                        refresh_needed = True

                        await self._broadcast({
                            "type": "exit",
                            "data": {"symbol": sym, "reason": reason}
                        })

                if refresh_needed:
                    await self._refresh_positions()

            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error(f"[router] exit_order_monitor error: {exc}")

    async def _broadcast(self, event: dict) -> None:
        for q in self._broadcast_queues:
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                pass

    # ── Manual controls (wired to API / dashboard) ────────────────────────────

    async def close_all_positions(self) -> None:
        broker = self.brokers.get(self.primary_broker)
        if not broker:
            return
        for pos in self._positions:
            ok = await broker.close_position(pos.symbol)
            if ok:
                self._position_params.pop(pos.symbol, None)
                self._submitted_at.pop(pos.symbol, None)
        await self._refresh_positions()
        logger.warning("[router] all positions closed manually")

    async def engage_kill_switch(self) -> None:
        self.risk.engage_kill_switch()
        self._save_state()
        logger.warning("[router] kill switch ENGAGED")
        await self._broadcast({"type": "kill_switch", "data": {"active": True}})

    async def disengage_kill_switch(self) -> None:
        self.risk.disengage_kill_switch()
        self._save_state()
        logger.info("[router] kill switch disengaged")
        await self._broadcast({"type": "kill_switch", "data": {"active": False}})

    @property
    def available_brokers(self) -> list[str]:
        return [b.value for b in self.brokers]

    async def switch_broker(self, name: BrokerName) -> None:
        if name not in self.brokers:
            raise ValueError(
                f"Broker '{name.value}' not connected; "
                f"available: {self.available_brokers}"
            )
        self.primary_broker = name
        logger.info(f"[router] primary broker → {name.value}")
        # Include env so the frontend can update brokerKey immediately on switch
        env = "live"
        if name == BrokerName.OANDA:
            env = "practice" if settings.oanda_practice else "live"
        elif name == BrokerName.ALPACA:
            env = "paper" if settings.alpaca_paper else "live"
        await self._broadcast({"type": "broker_switch", "data": {"broker": name.value, "env": env}})

    async def switch_trading_mode(self, mode: TradingMode) -> None:
        self.trading_mode     = mode
        kats_cfg.trading_mode = mode
        kats_cfg.save()
        self._save_state()
        logger.info(f"[router] trading mode → {mode.value}")
        await self._broadcast({"type": "mode_switch", "data": {"mode": mode.value}})

    async def update_instruments(self, instruments: list[str]) -> None:
        """
        Apply a new instrument list at runtime:
          - Save to kats_config.json
          - Update rt.symbols (router filter)
          - Update all feed symbol lists (affects next bar poll)
          - Update price feed symbols (affects next price tick)
          - Prefetch history for any newly added symbols
        """
        old_set = set(self.symbols)
        new_set = set(instruments)
        added   = new_set - old_set

        self.symbols          = list(instruments)
        kats_cfg.instruments  = ",".join(instruments)
        kats_cfg.save()

        # Update price feed (immediate — stream reconnects with new instrument list)
        if self._price_feed is not None:
            if hasattr(self._price_feed, "update_symbols"):
                self._price_feed.update_symbols(list(instruments))
            else:
                self._price_feed.symbols = list(instruments)

        # Update bar feeds (takes effect next _stream() iteration)
        for feed in self.pipeline.feeds:
            feed.update_symbols(instruments)

        await self._broadcast({
            "type": "instruments_update",
            "data": {"instruments": instruments},
        })
        logger.info(f"[router] instruments → {instruments}")

        # Prefetch history for newly added symbols in background
        if added:
            asyncio.create_task(
                self.pipeline.prefetch_symbols(list(added)),
                name="symbol-prefetch",
            )

    async def switch_timeframe(self, timeframe: str) -> None:
        self.timeframe             = timeframe
        kats_cfg.default_timeframe = timeframe
        kats_cfg.save()
        self._save_state()
        logger.info(f"[router] timeframe → {timeframe}")
        await self._broadcast({"type": "timeframe_switch", "data": {"timeframe": timeframe}})
        # Clear stale candles and refetch at new timeframe (background, non-blocking)
        asyncio.create_task(
            self._reload_history_task(timeframe), name="history-reload"
        )

    async def _reload_history_task(self, timeframe: str) -> None:
        """Background: clear history buffers and re-prefetch at new timeframe."""
        try:
            logger.info(f"[router] reloading history for tf={timeframe}…")
            await self.pipeline.reload_history(timeframe, self.symbols)
            logger.success(f"[router] ✓ history reloaded | tf={timeframe}")
        except Exception as exc:
            logger.error(f"[router] history reload failed: {exc}")

    async def pause_kronos(self) -> None:
        self.kronos_paused = True
        self._save_state()
        logger.warning("[router] Kronos inference PAUSED — no new signals will be generated")
        await self._broadcast({"type": "kronos_pause", "data": {"paused": True}})

    async def resume_kronos(self) -> None:
        self.kronos_paused = False
        self._save_state()
        logger.info("[router] Kronos inference RESUMED")
        await self._broadcast({"type": "kronos_pause", "data": {"paused": False}})

    async def update_kronos_settings(self, cfg: dict) -> None:
        """
        Apply new Kronos model settings at runtime:
          1. Pause inference
          2. Persist new values to kats_config.json
          3. Replace KronosEngine + SignalGenerator with fresh instances
          4. Load model in background; resume when ready
        """
        logger.info(f"[router] Kronos settings update → {cfg}")
        await self.pause_kronos()
        await self._broadcast({
            "type": "kronos_restart",
            "data": {"status": "restarting", "settings": cfg},
        })

        # Persist only known Kronos keys
        _valid = {
            "kronos_model_size", "kronos_device",
            "kronos_max_context", "kronos_forecast_horizon", "kronos_mc_samples",
        }
        for k, v in cfg.items():
            if k in _valid and hasattr(kats_cfg, k):
                setattr(kats_cfg, k, v)
        kats_cfg.save()

        # Replace engine — old instance stays alive until GC
        self.kronos  = KronosEngine()
        self.signals = SignalGenerator(self.kronos, self.pipeline)

        asyncio.create_task(self._reload_kronos_task(), name="kronos-reload")

    async def _reload_kronos_task(self) -> None:
        """Background task: load new Kronos weights then resume inference."""
        try:
            await self.kronos.load()
            await self.resume_kronos()
            snapshot = {
                "model_size":       kats_cfg.kronos_model_size,
                "device":           kats_cfg.kronos_device,
                "max_context":      kats_cfg.kronos_max_context,
                "forecast_horizon": kats_cfg.kronos_forecast_horizon,
                "mc_samples":       kats_cfg.kronos_mc_samples,
            }
            await self._broadcast({
                "type": "kronos_restart",
                "data": {"status": "ready", "settings": snapshot},
            })
            logger.success("[router] ✓ Kronos reloaded with new settings")
        except Exception as exc:
            logger.error(f"[router] Kronos reload failed: {exc}")
            await self._broadcast({
                "type": "kronos_restart",
                "data": {"status": "error", "message": str(exc)},
            })

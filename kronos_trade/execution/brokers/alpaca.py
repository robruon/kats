"""
kronos_trade/execution/brokers/alpaca.py
Alpaca broker adapter using alpaca-py.
Supports crypto and equities, paper and live.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from alpaca.trading.client import TradingClient
from alpaca.trading.enums import (
    AssetClass as AlpacaAssetClass,
    AssetStatus,
    OrderSide as AlpacaSide,
    OrderType,
    TimeInForce
)
from alpaca.trading.models import Position as AlpacaPosition
from alpaca.trading.requests import (
    GetAssetsRequest,
    GetOrdersRequest,
    LimitOrderRequest,
    MarketOrderRequest,
    StopLimitOrderRequest,
    StopLossRequest,
    TakeProfitRequest
)
from alpaca.trading.enums import OrderStatus as AlpacaOrderStatus, QueryOrderStatus
from loguru import logger

from kronos_trade.config import settings
from kronos_trade.models import (
    AccountSnapshot, BracketOrder, BrokerName, Direction,
    OrderSide, OrderStatus, Position,
    to_alpaca_symbol, from_alpaca_symbol, register_crypto_symbol,
    AssetClass, asset_class as get_asset_class
)

from .base import BrokerAdapter


class AlpacaAdapter(BrokerAdapter):

    def __init__(self) -> None:
        self._client: TradingClient | None = None
        self._tradeable: set[str] = set()
        # Maps internal symbol (e.g. "ETHUSD") → Alpaca asset UUID
        # Crypto symbols contain "/" which breaks URL paths — use UUID for close calls
        self._asset_ids: dict[str, str] = {}

    @property
    def name(self) -> BrokerName:
        return BrokerName.ALPACA

    @property
    def supported_symbols(self) -> set[str]:
        return self._tradeable

    async def connect(self) -> None:
        self._client = TradingClient(
            api_key=settings.alpaca_api_key,
            secret_key=settings.alpaca_secret_key,
            paper=settings.alpaca_paper,
        )
        acct = self._client.get_account()
        logger.info(
            f"[alpaca] connected | equity=${float(acct.equity):,.2f} "
            f"paper={settings.alpaca_paper}"
        )

        # Fetch live tradeable symbols so the router can filter upfront
        self._tradeable = await self._fetch_tradeable()
        logger.info(f"[alpaca] {len(self._tradeable)} tradeable symbols loaded")

    async def _fetch_tradeable(self) -> set[str]:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._fetch_tradeable_sync)

    def _fetch_tradeable_sync(self) -> set[str]:
        symbols: set[str] = set()
        for ac in (AlpacaAssetClass.US_EQUITY, AlpacaAssetClass.CRYPTO):
            try:
                req    = GetAssetsRequest(status=AssetStatus.ACTIVE, asset_class=ac)
                assets = self._client.get_all_assets(req)
                for asset in assets:
                    if not asset.tradable or not asset.symbol:
                        continue
                    internal = from_alpaca_symbol(asset.symbol)   # "SOL/USD" → "SOLUSD"
                    symbols.add(internal)
                    if ac == AlpacaAssetClass.CRYPTO:
                        # Register slash format so to_alpaca_symbol() works for every
                        # pair Alpaca supports (BTC/USDT, ETH/BTC, SOL/USDC, …)
                        register_crypto_symbol(internal, asset.symbol)
                        # Cache UUID — slash in symbol breaks URL path for close calls
                        if asset.id:
                            self._asset_ids[internal] = str(asset.id)
            except Exception as exc:
                logger.warning(f"[alpaca] could not fetch {ac} assets: {exc}")

        logger.info(f"[alpaca] registered {sum(1 for s in symbols if '/' not in s)} tradeable symbols")
        return symbols

    async def disconnect(self) -> None:
        self._client = None
        logger.info("[alpaca] disconnected")

    async def submit_bracket_order(self, order: BracketOrder) -> BracketOrder:
        if not self._client:
            raise RuntimeError("AlpacaAdapter not connected")

        side = AlpacaSide.BUY if order.side == OrderSide.BUY else AlpacaSide.SELL
        is_crypto = get_asset_class(order.symbol) == AssetClass.CRYPTO

        try:
            if is_crypto:
                if side == AlpacaSide.SELL:
                    order.status = OrderStatus.REJECTED
                    order.reject_reason = (
                        f"Alpaca does not support crypto short selling ({order.symbol})"
                    )
                    logger.warning(f"[alpaca] {order.reject_reason}")
                    return order

                else:
                    req = MarketOrderRequest(
                        symbol=to_alpaca_symbol(order.symbol),
                        qty=order.quantity,
                        side=side,
                        time_in_force=TimeInForce.GTC
                    )
                    submitted = self._client.submit_order(req)
                    order.broker_order_id = str(submitted.id)
                    order.status          = OrderStatus.SUBMITTED
                    logger.info(
                        f"[alpaca] crypto market entry {order.symbol} id={submitted.id} "
                        "(stop/target managed by price monitor)"
                    )
            else:
                req = MarketOrderRequest(
                    symbol=to_alpaca_symbol(order.symbol),
                    qty=order.quantity,
                    side=side,
                    time_in_force=TimeInForce.GTC,
                    order_class="bracket",
                    stop_loss=StopLossRequest(stop_price=round(order.stop_loss, 4)),
                    take_profit=TakeProfitRequest(limit_price=round(order.take_profit, 4)),
                )
                submitted = self._client.submit_order(req)
                order.broker_order_id = str(submitted.id)
                order.status          = OrderStatus.SUBMITTED
                logger.info(f"[alpaca] bracket order {order.symbol} id={submitted.id}")
        except Exception as exc:
            order.status = OrderStatus.REJECTED
            msg = str(exc).lower()
            if any(w in msg for w in ("not found", "invalid symbol", "not tradeable", "asset")):
                logger.warning(f"[alpaca] {order.symbol} not tradeable on this account: {exc}")
            else:
                logger.error(f"[alpaca] order rejected: {exc}")

        return order

    async def cancel_order(self, broker_order_id: str) -> bool:
        if not self._client:
            return False
        try:
            self._client.cancel_order_by_id(broker_order_id)
            return True
        except Exception as exc:
            logger.error(f"[alpaca] cancel failed: {exc}")
            return False

    async def get_order_fill(self, order_id: str) -> tuple[float, float] | None:
        """
        Return (filled_qty, avg_fill_price) if the order is filled, else None.
        Runs in an executor so it doesn't block the event loop.
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._get_order_fill_sync, order_id)

    def _get_order_fill_sync(self, order_id: str) -> tuple[float, float] | None:
        try:
            order = self._client.get_order_by_id(order_id)
            if order.status in (AlpacaOrderStatus.FILLED, AlpacaOrderStatus.PARTIALLY_FILLED):
                qty   = float(order.filled_qty   or 0)
                price = float(order.filled_avg_price or 0)
                return (qty, price) if qty > 0 else None
        except Exception as exc:
            logger.debug(f"[alpaca] get_order_fill {order_id}: {exc}")
        return None

    async def submit_exit_orders(
        self,
        symbol: str,
        qty: float,
        direction: str,
        sl_price: float,
        tp_price: float,
    ) -> tuple[str, str]:
        """
        Submit broker-side TP (limit) and SL (stop-limit) exit orders for a crypto position.
        Returns (tp_order_id, sl_order_id).
        Raises on failure so the caller can fall back to the price monitor.
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            self._submit_exit_orders_sync,
            symbol, qty, direction, sl_price, tp_price,
        )

    def _submit_exit_orders_sync(
        self,
        symbol: str,
        qty: float,
        direction: str,
        sl_price: float,
        tp_price: float,
    ) -> tuple[str, str]:
        alpaca_sym = to_alpaca_symbol(symbol)
        is_long    = direction == "long"
        exit_side  = AlpacaSide.SELL if is_long else AlpacaSide.BUY

        # TP: limit order at target price
        tp_req = LimitOrderRequest(
            symbol=alpaca_sym,
            qty=round(qty, 8),
            side=exit_side,
            time_in_force=TimeInForce.GTC,
            limit_price=round(tp_price, 4),
        )
        tp_order = self._client.submit_order(tp_req)
        logger.info(f"[alpaca] {symbol} TP limit order @ {tp_price:.4f} id={tp_order.id}")

        # SL: stop-limit order — stop triggers at sl_price, limit gives small slippage buffer
        sl_limit = round(sl_price * 0.998, 4) if is_long else round(sl_price * 1.002, 4)
        sl_req = StopLimitOrderRequest(
            symbol=alpaca_sym,
            qty=round(qty, 8),
            side=exit_side,
            time_in_force=TimeInForce.GTC,
            stop_price=round(sl_price, 4),
            limit_price=sl_limit,
        )
        sl_order = self._client.submit_order(sl_req)
        logger.info(f"[alpaca] {symbol} SL stop-limit @ {sl_price:.4f} (lim={sl_limit:.4f}) id={sl_order.id}")

        return str(tp_order.id), str(sl_order.id)

    async def cancel_orders_for_symbol(self, symbol: str) -> None:
        """Cancel all open orders for a symbol (used when a position closes)."""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._cancel_open_orders_sync, symbol)

    async def get_positions(self) -> list[Position]:
        if not self._client:
            return []
        try:
            raw: list[AlpacaPosition] = self._client.get_all_positions()
            positions = []
            for p in raw:
                internal_sym = from_alpaca_symbol(p.symbol)
                # Cache asset_id for use in close_position (avoids slash-in-URL bug)
                if p.asset_id:
                    self._asset_ids[internal_sym] = str(p.asset_id)
                positions.append(self._convert_position(p))
            return positions
        except Exception as exc:
            logger.error(f"[alpaca] get_positions error: {exc}")
            return []

    async def get_account(self) -> AccountSnapshot:
        if not self._client:
            raise RuntimeError("AlpacaAdapter not connected")
        acct = self._client.get_account()
        f = self._acct_float

        return AccountSnapshot(
            broker=BrokerName.ALPACA,
            equity=f(acct, "equity"),
            cash=f(acct, "cash"),
            buying_power=f(acct, "buying_power"),
            daily_pnl=f(acct, "daily_profit_loss", "equity") - f(acct, "last_equity"),
            unrealized_pnl=f(acct, "unrealized_pl", "unrealized_profit_loss")
        )

    def _position_identifier(self, symbol: str) -> str:
        """
        Return the identifier to use for position API calls.
        Crypto symbols like 'ETH/USD' contain a slash that breaks URL paths,
        so we use the asset UUID instead when available.
        """
        return self._asset_ids.get(symbol) or to_alpaca_symbol(symbol)

    async def close_position(self, symbol: str) -> bool:
        """Close the entire position for a symbol."""
        if not self._client:
            return False
        loop = asyncio.get_event_loop()
        try:
            await loop.run_in_executor(None, self._cancel_open_orders_sync, symbol)
            identifier = self._position_identifier(symbol)
            await loop.run_in_executor(None, self._client.close_position, identifier)
            logger.info(f"[alpaca] closed position {symbol}")
            return True
        except Exception as exc:
            logger.error(f"[alpaca] close_position {symbol} failed: {exc}")
            return False

    async def close_position_qty(self, symbol: str, qty: float) -> bool:
        """
        Partially close a position by submitting a market order for `qty` units.
        Used for multi-entry scenarios where each entry has its own TP/SL and
        only one entry's exit should be triggered at a time.
        """
        if not self._client:
            return False
        from alpaca.trading.requests import ClosePositionRequest
        loop       = asyncio.get_event_loop()
        identifier = self._position_identifier(symbol)
        try:
            req = ClosePositionRequest(qty=str(round(qty, 8)))
            await loop.run_in_executor(
                None,
                lambda: self._client.close_position(identifier, close_options=req)
            )
            logger.info(f"[alpaca] partial close {symbol} qty={qty}")
            return True
        except Exception as exc:
            logger.error(f"[alpaca] close_position_qty {symbol} qty={qty} failed: {exc}")
            return False

    def _cancel_open_orders_sync(self, symbol: str) -> None:
        """Cancel any open orders for symbol before closing the position."""
        try:
            alpaca_sym = to_alpaca_symbol(symbol)
            req = GetOrdersRequest(
                status=QueryOrderStatus.OPEN,
                symbols=[alpaca_sym],
            )
            orders = self._client.get_orders(req)
            for order in orders:
                try:
                    self._client.cancel_order_by_id(str(order.id))
                    logger.debug(f"[alpaca] cancelled open order {order.id} for {symbol}")
                except Exception as e:
                    logger.debug(f"[alpaca] cancel order {order.id} skipped: {e}")
        except Exception as exc:
            logger.debug(f"[alpaca] cancel_open_orders for {symbol}: {exc}")

    @staticmethod
    def _convert_position(p: AlpacaPosition) -> Position:
        qty = float(p.qty)
        direction = Direction.LONG if qty >= 0 else Direction.SHORT
        entry = float(p.avg_entry_price)
        current = float(p.current_price or entry)
        return Position(
            symbol=from_alpaca_symbol(p.symbol),
            direction=direction,
            quantity=abs(qty),
            entry_price=entry,
            current_price=current,
            stop_loss=0.0,    # Alpaca doesn't expose these in position objects
            take_profit=0.0,
            broker=BrokerName.ALPACA,
            opened_at=datetime.now(tz=timezone.utc),
            unrealized_pnl=float(p.unrealized_pl or 0),
        )

    @staticmethod
    def _acct_float(acct, *attrs: str, default: float = 0.0) -> float:
        for attr in attrs:
            val = getattr(acct, attr, None)
            if val is not None:
                try: return float(val)
                except (TypeError, ValueError): continue

        return default

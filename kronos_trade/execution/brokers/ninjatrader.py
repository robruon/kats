"""
kronos_trade/execution/brokers/ninjatrader.py
NinjaTrader 8 broker adapter via a local HTTP webhook.

Architecture:
  This adapter POSTs JSON commands to a lightweight HTTP listener that runs
  inside NinjaTrader 8 as a NinjaScript strategy (NinjaTrader.WebhookBridge).
  The NinjaScript strategy polls this endpoint and translates commands into
  native NT8 orders via Rithmic/Tradovate/Continuum.

NT8-side setup:
  See scripts/nt8_webhook_bridge.cs for the NinjaScript companion strategy.
  It listens on http://localhost:{port}/command and processes:
    { "action": "ENTER_LONG|ENTER_SHORT|EXIT|CANCEL", "symbol": "...",
      "qty": N, "stop": N, "target": N }
"""
from __future__ import annotations

from datetime import datetime, timezone

import httpx
from loguru import logger

from kronos_trade.config import settings
from kronos_trade.models import (
    AccountSnapshot, BracketOrder, BrokerName, Direction,
    OrderSide, OrderStatus, Position,
)

from .base import BrokerAdapter

_BASE_URL_TPL = "http://{host}:{port}"


class NinjaTraderAdapter(BrokerAdapter):
    """
    Sends JSON commands to the NT8 Webhook Bridge NinjaScript strategy.
    """

    def __init__(self) -> None:
        self._base = _BASE_URL_TPL.format(
            host=settings.nt8_webhook_host,
            port=settings.nt8_webhook_port,
        )
        self._client: httpx.AsyncClient | None = None

    @property
    def name(self) -> BrokerName:
        return BrokerName.NINJATRADER

    async def connect(self) -> None:
        self._client = httpx.AsyncClient(base_url=self._base, timeout=10.0)
        try:
            r = await self._client.get("/ping")
            r.raise_for_status()
            logger.info(f"[ninjatrader] connected to bridge @ {self._base}")
        except Exception as exc:
            logger.warning(
                f"[ninjatrader] bridge unreachable @ {self._base}: {exc}\n"
                "Ensure the NT8 WebhookBridge strategy is running."
            )

    async def disconnect(self) -> None:
        if self._client:
            await self._client.aclose()
        self._client = None
        logger.info("[ninjatrader] disconnected")

    async def submit_bracket_order(self, order: BracketOrder) -> BracketOrder:
        if not self._client:
            order.status = OrderStatus.REJECTED
            return order

        action = "ENTER_LONG" if order.side == OrderSide.BUY else "ENTER_SHORT"
        payload = {
            "action":   action,
            "symbol":   order.symbol,
            "qty":      order.quantity,
            "stop":     round(order.stop_loss, 4),
            "target":   round(order.take_profit, 4),
            "account":  settings.nt8_account_id,
        }

        try:
            r = await self._client.post("/command", json=payload)
            r.raise_for_status()
            data = r.json()
            order.broker_order_id = data.get("order_id", "nt8-unknown")
            order.status          = OrderStatus.SUBMITTED
            logger.info(f"[ninjatrader] submitted {order.symbol} id={order.broker_order_id}")
        except Exception as exc:
            order.status = OrderStatus.REJECTED
            logger.error(f"[ninjatrader] submit failed: {exc}")

        return order

    async def cancel_order(self, broker_order_id: str) -> bool:
        if not self._client:
            return False
        try:
            r = await self._client.post("/command", json={
                "action":   "CANCEL",
                "order_id": broker_order_id,
                "account":  settings.nt8_account_id,
            })
            r.raise_for_status()
            return True
        except Exception as exc:
            logger.error(f"[ninjatrader] cancel {broker_order_id} failed: {exc}")
            return False

    async def get_positions(self) -> list[Position]:
        if not self._client:
            return []
        try:
            r = await self._client.get("/positions", params={"account": settings.nt8_account_id})
            r.raise_for_status()
            raw: list[dict] = r.json()
            return [self._parse_position(p) for p in raw]
        except Exception as exc:
            logger.error(f"[ninjatrader] get_positions failed: {exc}")
            return []

    async def get_account(self) -> AccountSnapshot:
        if not self._client:
            raise RuntimeError("NinjaTraderAdapter not connected")
        r = await self._client.get("/account", params={"id": settings.nt8_account_id})
        r.raise_for_status()
        data = r.json()
        return AccountSnapshot(
            broker=BrokerName.NINJATRADER,
            equity=float(data.get("equity", 0)),
            cash=float(data.get("cash_value", 0)),
            buying_power=float(data.get("buying_power", 0)),
            daily_pnl=float(data.get("today_pnl", 0)),
        )

    async def close_position(self, symbol: str) -> bool:
        if not self._client:
            return False
        try:
            r = await self._client.post("/command", json={
                "action":  "EXIT",
                "symbol":  symbol,
                "account": settings.nt8_account_id,
            })
            r.raise_for_status()
            logger.info(f"[ninjatrader] closed position {symbol}")
            return True
        except Exception as exc:
            logger.error(f"[ninjatrader] close_position {symbol} failed: {exc}")
            return False

    @staticmethod
    def _parse_position(data: dict) -> Position:
        qty = float(data.get("quantity", 0))
        direction = Direction.LONG if data.get("market_position") == "Long" else Direction.SHORT
        entry = float(data.get("average_price", 0))
        return Position(
            symbol=data.get("instrument", "UNKNOWN"),
            direction=direction,
            quantity=abs(qty),
            entry_price=entry,
            current_price=float(data.get("last_price", entry)),
            stop_loss=0.0,
            take_profit=0.0,
            broker=BrokerName.NINJATRADER,
            opened_at=datetime.now(tz=timezone.utc),
            unrealized_pnl=float(data.get("unrealized_pnl", 0)),
        )

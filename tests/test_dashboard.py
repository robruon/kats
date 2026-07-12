from __future__ import annotations

from datetime import datetime, timezone

import pytest

from kronos_trade.dashboard.tui import KronosTradeTUI
from kronos_trade.execution.router import ExecutionRouter
from kronos_trade.models import BrokerName, Direction, Position


def _position(symbol: str, pnl: float, current_price: float) -> dict:
    return {
        "symbol": symbol,
        "direction": "long",
        "quantity": 1.0,
        "entry_price": 100.0,
        "current_price": current_price,
        "stop_loss": 95.0,
        "take_profit": 110.0,
        "unrealized_pnl": pnl,
    }


class DashboardAppHarness(KronosTradeTUI):
    def on_mount(self) -> None:
        pass


class FakePipeline:
    def subscribe(self):
        raise NotImplementedError


class FakeBroker:
    name = BrokerName.ALPACA

    def __init__(self, positions: list[Position]) -> None:
        self._positions = positions

    async def connect(self) -> None:
        return None

    async def disconnect(self) -> None:
        return None

    async def submit_bracket_order(self, order):
        return order

    async def cancel_order(self, broker_order_id: str) -> bool:
        return True

    async def get_positions(self) -> list[Position]:
        return self._positions

    async def get_account(self):
        raise NotImplementedError

    async def close_position(self, symbol: str) -> bool:
        return True


class FakePriceFeed:
    def __init__(self, price: float) -> None:
        self._price = price

    def latest(self, symbol: str) -> float:
        return self._price


@pytest.mark.asyncio
async def test_router_refresh_positions_broadcasts_positions_event():
    positions = [
        Position(
            symbol="BTCUSD",
            direction=Direction.LONG,
            quantity=2.0,
            entry_price=100.0,
            current_price=100.0,
            stop_loss=0.0,
            take_profit=0.0,
            broker=BrokerName.ALPACA,
            opened_at=datetime.now(timezone.utc),
        )
    ]
    router = ExecutionRouter(
        pipeline=FakePipeline(),
        brokers=[FakeBroker(positions)],
        price_feed=FakePriceFeed(105.0),
    )

    queue = router.subscribe_broadcasts()
    await router._refresh_positions()
    event = queue.get_nowait()

    assert event["type"] == "positions"
    assert event["data"][0]["symbol"] == "BTCUSD"
    assert event["data"][0]["current_price"] == 105.0
    assert event["data"][0]["unrealized_pnl"] == 10.0


@pytest.mark.asyncio
async def test_tui_state_sync_updates_kill_switch_without_posting(monkeypatch):
    app = DashboardAppHarness()
    calls: list[bool] = []

    async def fake_post(path: str, json: dict):
        calls.append(json["engage"])

    monkeypatch.setattr(app._http, "post", fake_post)

    async with app.run_test() as pilot:
        app._apply_state(
            {
                "running": True,
                "kronos_loaded": True,
                "kill_switch": True,
                "positions": [],
                "daily_loss_halted": False,
                "drawdown_halted": False,
            }
        )
        await pilot.pause()
        assert app.query_one("#kill-switch-toggle").value is True
        assert calls == []

    await app._http.aclose()


@pytest.mark.asyncio
async def test_tui_positions_event_updates_unrealized_and_preserves_selection():
    app = DashboardAppHarness()

    async with app.run_test() as pilot:
        app._apply_account(
            {
                "equity": 10_000.0,
                "cash": 8_000.0,
                "daily_pnl": 12.0,
                "unrealized_pnl": 0.0,
            }
        )
        app._apply_state(
            {
                "running": True,
                "kronos_loaded": True,
                "kill_switch": False,
                "positions": [_position("BTCUSD", 5.0, 105.0), _position("ETHUSD", -2.0, 98.0)],
                "daily_loss_halted": False,
                "drawdown_halted": False,
            }
        )
        await pilot.pause()

        table = app.query_one("#positions-table")
        table.move_cursor(row=1, animate=False, scroll=False)
        app._handle_event(
            {
                "type": "positions",
                "data": [_position("BTCUSD", 6.5, 106.5), _position("ETHUSD", -1.5, 98.5)],
            }
        )
        await pilot.pause()

        assert table.selected_symbol() == "ETHUSD"
        assert "Unreal P&L: $       +5.00" in app.query_one("#acct-unreal-pnl").renderable.plain

    await app._http.aclose()


@pytest.mark.asyncio
async def test_tui_pressing_c_opens_close_bar_only_when_positions_exist():
    app = DashboardAppHarness()

    async with app.run_test() as pilot:
        await pilot.press("c")
        await pilot.pause()
        assert app._close_bar_open is False

        app._apply_state(
            {
                "running": True,
                "kronos_loaded": True,
                "kill_switch": False,
                "positions": [_position("BTCUSD", 3.0, 103.0)],
                "daily_loss_halted": False,
                "drawdown_halted": False,
            }
        )
        await pilot.pause()

        await pilot.press("c")
        await pilot.pause()
        assert app._close_bar_open is True
        assert app.query_one("#close-bar").display is True

    await app._http.aclose()

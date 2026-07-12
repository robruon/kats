"""
tests/test_risk.py
Unit tests for the risk gatekeeper — no broker or model needed.
"""
from __future__ import annotations

import pytest
from datetime import datetime, timezone

from kronos_trade.models import (
    BracketOrder, BrokerName, Direction, KronosSignal,
    OrderSide, OrderStatus, Position,
)
from kronos_trade.strategy.risk import RiskGatekeeper


def _make_position(symbol: str = "BTCUSD") -> Position:
    return Position(
        symbol=symbol,
        direction=Direction.LONG,
        quantity=0.01,
        entry_price=50_000.0,
        current_price=51_000.0,
        stop_loss=49_000.0,
        take_profit=53_000.0,
        broker=BrokerName.ALPACA,
        opened_at=datetime.now(tz=timezone.utc),
    )


def _make_order(symbol: str = "ETHUSD") -> BracketOrder:
    return BracketOrder(
        symbol=symbol,
        side=OrderSide.BUY,
        quantity=0.1,
        entry_price=3_000.0,
        stop_loss=2_900.0,
        take_profit=3_200.0,
        broker=BrokerName.ALPACA,
    )


def test_approve_clean_order():
    gk = RiskGatekeeper()
    gk.initialize(10_000.0)
    order = _make_order()
    approved, reason = gk.check_order(order, [], 10_000.0)
    assert approved, f"Expected approval, got: {reason}"
    assert reason == ""


def test_reject_kill_switch():
    gk = RiskGatekeeper()
    gk.initialize(10_000.0)
    gk.engage_kill_switch()
    order = _make_order()
    approved, reason = gk.check_order(order, [], 10_000.0)
    assert not approved
    assert "kill switch" in reason


def test_reject_daily_loss_halt():
    gk = RiskGatekeeper()
    gk.initialize(10_000.0)
    # Simulate 3% daily loss (limit is 2%)
    gk.update_equity(9_700.0)
    assert gk.state.daily_halted

    order = _make_order()
    approved, reason = gk.check_order(order, [], 9_700.0)
    assert not approved
    assert "daily loss" in reason


def test_reject_max_positions():
    gk = RiskGatekeeper()
    gk.initialize(10_000.0)
    # Fill up to max_concurrent_positions
    from kronos_trade.config import kats_cfg
    positions = [_make_position(f"SYM{i}") for i in range(kats_cfg.max_concurrent_positions)]
    order = _make_order("NEWONE")
    approved, reason = gk.check_order(order, positions, 10_000.0)
    assert not approved
    assert "max concurrent" in reason


def test_reject_duplicate_symbol():
    gk = RiskGatekeeper()
    gk.initialize(10_000.0)
    positions = [_make_position("ETHUSD")]
    order = _make_order("ETHUSD")
    approved, reason = gk.check_order(order, positions, 10_000.0)
    assert not approved
    assert "ETHUSD" in reason


def test_drawdown_halt():
    gk = RiskGatekeeper()
    gk.initialize(10_000.0)
    # Simulate 6% drawdown (limit is 5%)
    gk.update_equity(9_400.0)
    assert gk.state.drawdown_halted

    order = _make_order()
    approved, reason = gk.check_order(order, [], 9_400.0)
    assert not approved
    assert "drawdown" in reason


def test_kill_switch_toggle():
    gk = RiskGatekeeper()
    gk.initialize(10_000.0)
    gk.engage_kill_switch()
    assert gk.state.kill_switch

    gk.disengage_kill_switch()
    assert not gk.state.kill_switch

    order = _make_order()
    approved, _ = gk.check_order(order, [], 10_000.0)
    assert approved


def test_manual_daily_halt_reset():
    gk = RiskGatekeeper()
    gk.initialize(10_000.0)
    gk.update_equity(9_700.0)
    assert gk.state.daily_halted

    gk.reset_daily_halt()
    assert not gk.state.daily_halted

    order = _make_order()
    approved, _ = gk.check_order(order, [], 9_700.0)
    assert approved

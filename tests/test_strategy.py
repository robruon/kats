"""
tests/test_strategy.py
Unit tests for StrategyEngine position sizing — no model or broker needed.
"""
from __future__ import annotations

import pytest
from datetime import datetime, timezone

from kronos_trade.models import BrokerName, Direction, KronosSignal, OrderSide
from kronos_trade.strategy.engine import StrategyEngine


def _make_signal(
    direction: Direction = Direction.LONG,
    confidence: float = 0.75,
    entry: float = 50_000.0,
    atr: float = 500.0,
) -> KronosSignal:
    now = datetime.now(tz=timezone.utc)
    return KronosSignal(
        symbol="BTCUSD",
        generated_at=now,
        timeframe="1h",
        direction=direction,
        confidence=confidence,
        forecast_mean=[entry * 1.01] * 24,
        forecast_lower=[entry * 0.99] * 24,
        forecast_upper=[entry * 1.02] * 24,
        forecast_timestamps=[now] * 24,
        volatility_forecast=atr,
        entry_price=entry,
        atr=atr,
    )


def test_long_order_structure():
    eng = StrategyEngine()
    sig = _make_signal(direction=Direction.LONG, entry=50_000, atr=500)
    order = eng.build_order(sig, account_equity=10_000, broker=BrokerName.ALPACA)
    assert order is not None
    assert order.side == OrderSide.BUY
    assert order.stop_loss < order.entry_price
    assert order.take_profit > order.entry_price


def test_short_order_structure():
    eng = StrategyEngine()
    sig = _make_signal(direction=Direction.SHORT, entry=50_000, atr=500)
    order = eng.build_order(sig, account_equity=10_000, broker=BrokerName.ALPACA)
    assert order is not None
    assert order.side == OrderSide.SELL
    assert order.stop_loss > order.entry_price
    assert order.take_profit < order.entry_price


def test_flat_returns_none():
    eng = StrategyEngine()
    sig = _make_signal(direction=Direction.FLAT, confidence=0.5)
    order = eng.build_order(sig, account_equity=10_000)
    assert order is None


def test_rr_ratio_respected():
    eng = StrategyEngine()
    eng.rr_ratio = 3.0
    sig = _make_signal(direction=Direction.LONG, entry=100.0, atr=10.0)
    order = eng.build_order(sig, account_equity=10_000)
    assert order is not None
    sl_dist = order.entry_price - order.stop_loss
    tp_dist = order.take_profit - order.entry_price
    assert abs(tp_dist / sl_dist - 3.0) < 0.01, f"RR={tp_dist/sl_dist:.2f} expected 3.0"


def test_volatility_sizing_scales_with_atr():
    eng = StrategyEngine()
    eng.sizing_mode = "volatility"

    sig_low_vol  = _make_signal(entry=100.0, atr=1.0)
    sig_high_vol = _make_signal(entry=100.0, atr=5.0)

    order_lv = eng.build_order(sig_low_vol,  account_equity=10_000)
    order_hv = eng.build_order(sig_high_vol, account_equity=10_000)

    assert order_lv is not None and order_hv is not None
    # Higher volatility → smaller position (same risk budget, wider stop)
    assert order_hv.quantity < order_lv.quantity, (
        f"Expected smaller qty with high vol, got {order_hv.quantity} vs {order_lv.quantity}"
    )


def test_kelly_sizing_scales_with_confidence():
    eng = StrategyEngine()
    eng.sizing_mode = "kelly"

    sig_low_conf  = _make_signal(confidence=0.55, entry=100.0, atr=2.0)
    sig_high_conf = _make_signal(confidence=0.90, entry=100.0, atr=2.0)

    order_lc = eng.build_order(sig_low_conf,  account_equity=10_000)
    order_hc = eng.build_order(sig_high_conf, account_equity=10_000)

    assert order_lc is not None and order_hc is not None
    # Higher confidence → larger Kelly fraction → larger position
    assert order_hc.quantity > order_lc.quantity, (
        f"Expected larger qty with high confidence: {order_hc.quantity} vs {order_lc.quantity}"
    )

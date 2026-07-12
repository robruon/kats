"""
tests/test_pipeline.py
Unit tests for BarHistory + DataPipeline — no live feed needed.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone, timedelta

import pytest

from kronos_trade.data.pipeline import BarHistory, DataPipeline
from kronos_trade.data.feeds.base import BaseFeed
from kronos_trade.models import OHLCVBar


def _bar(symbol: str = "BTCUSD", close: float = 50_000.0, i: int = 0) -> OHLCVBar:
    return OHLCVBar(
        symbol=symbol,
        timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc) + timedelta(hours=i),
        open=close - 100,
        high=close + 200,
        low=close - 200,
        close=close,
        volume=1.5,
    )


# ── BarHistory tests ──────────────────────────────────────────────────────────

def test_bar_history_push_and_len():
    h = BarHistory(maxlen=10)
    for i in range(5):
        h.push(_bar(close=100 + i, i=i))
    assert len(h) == 5


def test_bar_history_maxlen():
    h = BarHistory(maxlen=5)
    for i in range(10):
        h.push(_bar(close=100 + i, i=i))
    assert len(h) == 5  # capped at maxlen
    assert h.last_close() == 109.0  # most recent


def test_bar_history_last_close():
    h = BarHistory()
    assert h.last_close() is None
    h.push(_bar(close=42_000.0, i=0))
    assert h.last_close() == 42_000.0


def test_bar_history_atr():
    h = BarHistory()
    for i in range(20):
        h.push(OHLCVBar(
            symbol="X",
            timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc) + timedelta(hours=i),
            open=100.0,
            high=110.0,
            low=90.0,
            close=100.0,
            volume=1.0,
        ))
    atr = h.atr(period=14)
    # Each bar TR = max(20, |110-100|, |90-100|) = 20
    assert abs(atr - 20.0) < 0.01


def test_bar_history_atr_insufficient_data():
    h = BarHistory()
    for i in range(5):
        h.push(_bar(i=i))
    assert h.atr(period=14) == 0.0   # not enough bars


def test_to_kronos_df_shape():
    h = BarHistory()
    for i in range(30):
        h.push(_bar(close=100 + i, i=i))
    df, ts = h.to_kronos_df()
    assert len(df) == 30
    assert len(ts) == 30
    assert set(["open", "high", "low", "close", "volume", "amount"]).issubset(df.columns)


# ── DataPipeline tests ────────────────────────────────────────────────────────

class MockFeed(BaseFeed):
    """Emits a fixed sequence of bars on start."""

    def __init__(self, bars: list[OHLCVBar]) -> None:
        super().__init__(symbols=[b.symbol for b in bars], timeframe="1h")
        self._bars_to_emit = bars

    @property
    def name(self) -> str:
        return "mock"

    async def connect(self) -> None:
        pass

    async def disconnect(self) -> None:
        pass

    async def _stream(self) -> None:
        for bar in self._bars_to_emit:
            await self._emit(bar)
            await asyncio.sleep(0)
        # Then idle until stopped
        while self._running:
            await asyncio.sleep(0.05)

    async def fetch_history(self, symbol: str, n_bars: int = 512) -> list[OHLCVBar]:
        return [b for b in self._bars_to_emit if b.symbol == symbol][:n_bars]


@pytest.mark.asyncio
async def test_pipeline_receives_bars():
    bars = [_bar("BTCUSD", close=50_000 + i, i=i) for i in range(5)]
    feed = MockFeed(bars)
    pipeline = DataPipeline(feeds=[feed], history_len=512)

    sub_queue = pipeline.subscribe()
    await pipeline.start()

    received = []
    for _ in range(5):
        try:
            bar = await asyncio.wait_for(sub_queue.get(), timeout=1.0)
            received.append(bar)
        except asyncio.TimeoutError:
            break

    await pipeline.stop()

    assert len(received) == 5
    assert all(b.symbol == "BTCUSD" for b in received)


@pytest.mark.asyncio
async def test_pipeline_history_prefetch():
    bars = [_bar("BTCUSD", close=100 + i, i=i) for i in range(20)]
    feed = MockFeed(bars)
    pipeline = DataPipeline(feeds=[feed], history_len=512)
    await pipeline.start()
    await asyncio.sleep(0.1)

    history = pipeline.history("BTCUSD")
    assert len(history) >= 20   # prefetch + live bars

    await pipeline.stop()


@pytest.mark.asyncio
async def test_pipeline_multiple_symbols():
    btc_bars = [_bar("BTCUSD", close=50_000 + i, i=i) for i in range(3)]
    eth_bars = [_bar("ETHUSD", close=3_000  + i, i=i) for i in range(3)]
    feed = MockFeed(btc_bars + eth_bars)
    pipeline = DataPipeline(feeds=[feed], history_len=512)

    sub = pipeline.subscribe()
    await pipeline.start()

    received_syms = set()
    for _ in range(6):
        try:
            bar = await asyncio.wait_for(sub.get(), timeout=1.0)
            received_syms.add(bar.symbol)
        except asyncio.TimeoutError:
            break

    await pipeline.stop()
    assert "BTCUSD" in received_syms
    assert "ETHUSD" in received_syms

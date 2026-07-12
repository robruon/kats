"""
kronos_trade/data/feeds/databento_feed.py
Databento live feed for futures and forex via DBN streaming.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import databento as db
from loguru import logger

from kronos_trade.config import settings
from kronos_trade.models import OHLCVBar

from .base import BaseFeed

# Map our friendly symbol names to Databento instrument IDs / datasets
SYMBOL_DATASET_MAP: dict[str, tuple[str, str]] = {
    "NQ":     ("GLBX.MDP3", "NQ.c.0"),   # CME E-mini NASDAQ continuous
    "MNQ":    ("GLBX.MDP3", "MNQ.c.0"),  # CME Micro E-mini NASDAQ continuous
    "ES":     ("GLBX.MDP3", "ES.c.0"),
    "XAUUSD": ("GLBX.MDP3", "GC.c.0"),   # Gold futures (proxy)
}

_TF_SECONDS: dict[str, int] = {
    "1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400,
}


class DatabentoBars:
    """Lightweight OHLCV bar builder from tick/trade data."""

    def __init__(self, symbol: str, tf_seconds: int) -> None:
        self.symbol     = symbol
        self.tf_seconds = tf_seconds
        self._reset()

    def _reset(self) -> None:
        self._open = self._high = self._low = self._close = self._volume = None
        self._bar_start: datetime | None = None

    def _bucket(self, ts: datetime) -> datetime:
        epoch = int(ts.timestamp())
        snapped = (epoch // self.tf_seconds) * self.tf_seconds
        return datetime.fromtimestamp(snapped, tz=timezone.utc)

    def on_trade(self, price: float, size: float, ts: datetime) -> OHLCVBar | None:
        """Feed a trade tick; returns a completed bar when the bucket rolls."""
        bucket = self._bucket(ts)
        completed: OHLCVBar | None = None

        if self._bar_start is not None and bucket != self._bar_start:
            # Emit completed bar
            completed = OHLCVBar(
                symbol=self.symbol,
                timestamp=self._bar_start,
                open=self._open, high=self._high,
                low=self._low,  close=self._close,
                volume=self._volume, timeframe="custom",
            )
            self._reset()

        if self._open is None:
            self._bar_start = bucket
            self._open = self._high = self._low = self._close = price
            self._volume = size
        else:
            self._high   = max(self._high, price)
            self._low    = min(self._low, price)
            self._close  = price
            self._volume += size

        return completed


class DatabentofFeed(BaseFeed):
    """
    Streams live trade data from Databento and assembles OHLCV bars.
    Supports futures (CME, CBOT, NYMEX) via GLBX.MDP3 dataset.
    """

    def __init__(self, symbols: list[str], timeframe: str = "1h") -> None:
        super().__init__(symbols, timeframe)
        self._client: db.Live | None = None
        self._tf_seconds = _TF_SECONDS.get(timeframe, 3600)
        self._builders: dict[str, DatabentoBars] = {}

    @property
    def name(self) -> str:
        return "databento"

    async def connect(self) -> None:
        self._client = db.Live(key=settings.databento_api_key)
        for sym in self.symbols:
            self._builders[sym] = DatabentoBars(sym, self._tf_seconds)

    async def disconnect(self) -> None:
        if self._client:
            try:
                self._client.stop()
            except Exception:
                pass

    async def _stream(self) -> None:
        if not self._client:
            return

        for sym in self.symbols:
            dataset, instrument = SYMBOL_DATASET_MAP.get(sym, ("GLBX.MDP3", sym))
            self._client.subscribe(
                dataset=dataset,
                schema="trades",
                symbols=[instrument],
            )

        self._client.start()
        loop = asyncio.get_event_loop()

        while self._running:
            try:
                record = await loop.run_in_executor(None, self._client.blocking_next, 0.1)
                if record is None:
                    await asyncio.sleep(0.01)
                    continue
                await self._handle_record(record)
            except StopIteration:
                break
            except Exception as exc:
                logger.error(f"[databento] record error: {exc}")
                await asyncio.sleep(0.1)

    async def _handle_record(self, record: object) -> None:
        try:
            ts    = datetime.fromtimestamp(record.ts_event / 1e9, tz=timezone.utc)
            price = record.price / 1e9   # Databento fixed-point
            size  = float(record.size)
            sym   = record.hd.instrument_id  # map back to our symbol

            # Find which of our symbols this matches
            for our_sym, (_, inst) in SYMBOL_DATASET_MAP.items():
                if our_sym in self.symbols:
                    builder = self._builders.get(our_sym)
                    if builder:
                        bar = builder.on_trade(price, size, ts)
                        if bar:
                            bar.symbol = our_sym
                            await self._emit(bar)
        except Exception as exc:
            logger.debug(f"[databento] handle_record skip: {exc}")

    async def fetch_history(self, symbol: str, n_bars: int = 512) -> list[OHLCVBar]:
        """Fetch historical OHLCV bars via Databento batch/timeseries API."""
        dataset, instrument = SYMBOL_DATASET_MAP.get(symbol, ("GLBX.MDP3", symbol))

        client   = db.Historical(key=settings.databento_api_key)
        schema   = db.Schema.OHLCV_1H if self.timeframe == "1h" else db.Schema.OHLCV_1M

        loop = asyncio.get_event_loop()
        data = await loop.run_in_executor(
            None,
            lambda: client.timeseries.get_range(
                dataset=dataset,
                symbols=[instrument],
                schema=schema,
                limit=n_bars,
            )
        )

        bars: list[OHLCVBar] = []
        for rec in data:
            ts = datetime.fromtimestamp(rec.ts_event / 1e9, tz=timezone.utc)
            bars.append(OHLCVBar(
                symbol=symbol,
                timestamp=ts,
                open=rec.open / 1e9,
                high=rec.high / 1e9,
                low=rec.low  / 1e9,
                close=rec.close / 1e9,
                volume=float(rec.volume),
                timeframe=self.timeframe,
            ))
        return bars

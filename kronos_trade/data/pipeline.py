"""
kronos_trade/data/pipeline.py
Multi-feed aggregator + rolling bar history.
Distributes completed bars to subscribers (Kronos engine, dashboard, etc.)
"""
from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from datetime import datetime

import pandas as pd
from loguru import logger

from kronos_trade.config import settings
from kronos_trade.models import OHLCVBar

from .feeds.base import BaseFeed


class BarHistory:
    """
    Rolling deque of OHLCVBar for a single symbol.
    Converts to the DataFrame format expected by KronosPredictor.
    """

    def __init__(self, maxlen: int = 512) -> None:
        self._bars: deque[OHLCVBar] = deque(maxlen=maxlen)

    def push(self, bar: OHLCVBar) -> None:
        self._bars.append(bar)

    def __len__(self) -> int:
        return len(self._bars)

    def to_kronos_df(self) -> tuple[pd.DataFrame, pd.Series]:
        """
        Returns (df, x_timestamp) ready for KronosPredictor.predict().
        df columns: open, high, low, close, volume, amount
        x_timestamp: pd.Series of timestamps
        """
        rows = list(self._bars)
        df = pd.DataFrame([{
            "open":   b.open,
            "high":   b.high,
            "low":    b.low,
            "close":  b.close,
            "volume": b.volume,
            "amount": b.amount,
        } for b in rows])
        timestamps = pd.Series([b.timestamp for b in rows])
        return df, timestamps

    def last_close(self) -> float | None:
        if self._bars:
            return self._bars[-1].close
        return None

    def last_bar(self) -> OHLCVBar | None:
        if self._bars:
            return self._bars[-1]
        return None

    def atr(self, period: int = 14) -> float:
        """Simple ATR calculation over recent bars."""
        bars = list(self._bars)
        if len(bars) < period + 1:
            return 0.0
        trs = []
        for i in range(1, len(bars)):
            high = bars[i].high
            low  = bars[i].low
            prev_close = bars[i - 1].close
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            trs.append(tr)
        recent_trs = trs[-period:]
        return sum(recent_trs) / len(recent_trs)


# ── Subscriber type ───────────────────────────────────────────────────────────

BarSubscriber = asyncio.Queue  # receives OHLCVBar objects


class DataPipeline:
    """
    Central data pipeline.
    - Accepts multiple BaseFeed instances
    - Maintains per-symbol BarHistory (rolling 512-bar window)
    - Fan-out to any number of async subscribers
    - Pre-populates history from feed.fetch_history() on startup
    """

    def __init__(
        self,
        feeds: list[BaseFeed],
        history_len: int | None = None,
    ) -> None:
        self.feeds       = feeds
        self.history_len = history_len or settings.kronos_max_context
        self._history:     dict[str, BarHistory]        = defaultdict(
            lambda: BarHistory(maxlen=self.history_len)
        )
        self._subscribers: list[asyncio.Queue[OHLCVBar]] = []
        self._tasks: list[asyncio.Task] = []
        self._running = False

    # ── Pub/sub ───────────────────────────────────────────────────────────────

    def subscribe(self) -> asyncio.Queue[OHLCVBar]:
        """Get a queue that receives every new completed bar."""
        q: asyncio.Queue[OHLCVBar] = asyncio.Queue(maxsize=500)
        self._subscribers.append(q)
        return q

    def history(self, symbol: str) -> BarHistory:
        return self._history[symbol]

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        if self._running:
            return
        self._running = True

        # Pre-populate history from each feed
        await self._prefetch_history()

        # Start all feeds
        for feed in self.feeds:
            await feed.start()

        # Drain each feed's queue into the pipeline
        for feed in self.feeds:
            task = asyncio.create_task(
                self._drain_feed(feed), name=f"drain-{feed.name}"
            )
            self._tasks.append(task)

        logger.info(f"[pipeline] started | feeds={[f.name for f in self.feeds]}")

    async def stop(self) -> None:
        self._running = False
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        for feed in self.feeds:
            await feed.stop()
        logger.info("[pipeline] stopped")

    # ── Internal ──────────────────────────────────────────────────────────────

    async def prefetch_symbols(self, symbols: list[str]) -> None:
        """
        Prefetch history for a set of symbols that were just added at runtime.
        Uses whichever feed supports fetch_history() for each symbol.
        """
        for sym in symbols:
            for feed in self.feeds:
                if sym not in feed.symbols:
                    continue
                try:
                    bars = await feed.fetch_history(sym, n_bars=self.history_len)
                    for bar in bars:
                        self._history[sym].push(bar)
                    logger.info(
                        f"[pipeline] prefetched {len(bars)} bars for new symbol {sym}"
                    )
                except NotImplementedError:
                    pass
                except Exception as exc:
                    logger.warning(f"[pipeline] prefetch {sym} failed: {exc}")
                break

    async def reload_history(self, timeframe: str, symbols: list[str] | None = None) -> None:
        """
        Update all feeds' timeframe, clear history buffers for the given symbols
        (or all known symbols), then re-prefetch at the new timeframe.
        Called after a runtime timeframe switch.
        """
        # Propagate new timeframe to every feed
        for feed in self.feeds:
            feed.update_timeframe(timeframe)

        # Determine which symbols to reload
        syms = symbols if symbols is not None else list(self._history.keys())

        # Clear stale candles
        for sym in syms:
            self._history[sym] = BarHistory(maxlen=self.history_len)

        # Re-prefetch with new timeframe
        for sym in syms:
            for feed in self.feeds:
                if sym not in feed.symbols:
                    continue
                try:
                    bars = await feed.fetch_history(sym, n_bars=self.history_len)
                    for bar in bars:
                        self._history[sym].push(bar)
                    logger.debug(f"[pipeline] reloaded {len(bars)} bars for {sym} @ {timeframe}")
                except NotImplementedError:
                    pass
                except Exception as exc:
                    logger.warning(f"[pipeline] reload {sym} @ {timeframe} failed: {exc}")
                break

        logger.info(f"[pipeline] history reloaded | tf={timeframe} symbols={syms}")

    async def _prefetch_history(self) -> None:
        """Fill rolling history buffers before going live."""
        all_symbols: set[str] = set()
        for feed in self.feeds:
            all_symbols.update(feed.symbols)

        for sym in all_symbols:
            for feed in self.feeds:
                if sym not in feed.symbols:
                    continue
                try:
                    bars = await feed.fetch_history(sym, n_bars=self.history_len)
                    for bar in bars:
                        self._history[sym].push(bar)
                    logger.info(
                        f"[pipeline] prefetched {len(bars)} bars for {sym} from {feed.name}"
                    )
                    break  # only need one source per symbol
                except NotImplementedError:
                    logger.debug(f"[pipeline] {feed.name} has no fetch_history for {sym}")
                except Exception as exc:
                    logger.warning(f"[pipeline] prefetch {sym} from {feed.name} failed: {exc}")

    async def _drain_feed(self, feed: BaseFeed) -> None:
        while self._running:
            try:
                bar = await asyncio.wait_for(feed.bar_queue.get(), timeout=1.0)
                self._on_bar(bar)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error(f"[pipeline] drain error from {feed.name}: {exc}")

    def _on_bar(self, bar: OHLCVBar) -> None:
        self._history[bar.symbol].push(bar)
        for q in self._subscribers:
            try:
                q.put_nowait(bar)
            except asyncio.QueueFull:
                pass   # subscriber can't keep up — drop and continue

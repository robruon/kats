"""
kronos_trade/data/feeds/base.py
Abstract base for all market data feeds.
Every feed produces OHLCVBar objects into an asyncio.Queue.
"""
from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod

from loguru import logger

from kronos_trade.models import OHLCVBar


class BaseFeed(ABC):
    """
    Implement `connect()`, `disconnect()`, and `_stream()`.
    The base class manages the task lifecycle and exposes `bar_queue`.
    """

    def __init__(self, symbols: list[str], timeframe: str = "1h") -> None:
        self.symbols   = symbols
        self.timeframe = timeframe
        self.bar_queue: asyncio.Queue[OHLCVBar] = asyncio.Queue(maxsize=1000)
        self._task: asyncio.Task | None = None
        self._running = False

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        await self.connect()
        self._task = asyncio.create_task(self._stream_loop(), name=f"{self.name}-stream")
        logger.info(f"[{self.name}] started | symbols={self.symbols} tf={self.timeframe}")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        await self.disconnect()
        logger.info(f"[{self.name}] stopped")

    # ── Internal ──────────────────────────────────────────────────────────────

    async def _stream_loop(self) -> None:
        while self._running:
            try:
                await self._stream()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error(f"[{self.name}] stream error: {exc} — reconnecting in 2s")
                try:
                    await asyncio.sleep(2)
                except asyncio.CancelledError:
                    break

    async def _emit(self, bar: OHLCVBar) -> None:
        """Push a completed bar onto the queue (non-blocking drop if full)."""
        try:
            self.bar_queue.put_nowait(bar)
        except asyncio.QueueFull:
            logger.warning(f"[{self.name}] bar_queue full — dropping bar for {bar.symbol}")

    # ── Abstract interface ────────────────────────────────────────────────────

    @property
    @abstractmethod
    def name(self) -> str: ...

    @abstractmethod
    async def connect(self) -> None: ...

    @abstractmethod
    async def disconnect(self) -> None: ...

    @abstractmethod
    async def _stream(self) -> None:
        """
        Run the inner feed loop. Should call `await self._emit(bar)` for
        each completed bar. Must respect `self._running`.
        """
        ...

    # ── Runtime updates ───────────────────────────────────────────────────────

    def update_symbols(self, symbols: list[str]) -> None:
        """Replace the active symbol list. Takes effect on the next stream iteration."""
        self.symbols = list(symbols)

    def update_timeframe(self, timeframe: str) -> None:
        """Replace the active timeframe. Takes effect on the next fetch_history() call."""
        self.timeframe = timeframe

    # ── Historical fetch (optional) ───────────────────────────────────────────

    async def fetch_history(
        self,
        symbol: str,
        n_bars: int = 512,
    ) -> list[OHLCVBar]:
        """
        Override to support fetching historical bars for Kronos context window.
        Default raises NotImplementedError.
        """
        raise NotImplementedError(f"{self.name} does not implement fetch_history()")

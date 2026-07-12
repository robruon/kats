"""
Alpaca WebSocket feed — crypto and equities.
Uses alpaca-py's async streaming client.
"""
from __future__ import annotations

import asyncio
import json
import time as _time
from datetime import datetime, timezone

import pandas as pd
import websockets
from loguru import logger

from kronos_trade.config import settings
from kronos_trade.models import (
   AssetClass, OHLCVBar, asset_class,
   from_alpaca_symbol, to_alpaca_symbol
)

from .base import BaseFeed

_CRYPTO_WS = "wss://stream.data.alpaca.markets/v1beta3/crypto/us"
_STOCK_WS  = "wss://stream.data.alpaca.markets/v2/{feed}"

# ── Timeframe helpers ─────────────────────────────────────────────────────────
_TF_MAP = {
    "1m":  60,
    "5m":  300,
    "15m": 900,
    "1h":  3600,
    "4h":  14400,
    "1d":  86400,
}

def _seconds_until_next_bar(tf_seconds: int, offset: int = 5) -> float:
    """
    Seconds until the next bar bar close, plus `offset` seconds buffer
    to ensure the bar is finalized before we fetch it.
    """
    now = _time.time()
    next_close = (int(now / tf_seconds) + 1) * tf_seconds
    return max(0.0, next_close - now + offset)

def _alpaca_timeframe(tf: str):
    """Return alpaca-py TimeFrame object for a timeframe string."""
    from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
    mapping = {
        "1m":  TimeFrame(1,  TimeFrameUnit.Minute),
        "5m":  TimeFrame(5,  TimeFrameUnit.Minute),
        "15m": TimeFrame(15, TimeFrameUnit.Minute),
        "1h":  TimeFrame(1,  TimeFrameUnit.Hour),
        "4h":  TimeFrame(4,  TimeFrameUnit.Hour),
        "1d":  TimeFrame(1,  TimeFrameUnit.Day),
    }
    return mapping.get(tf, TimeFrame(1, TimeFrameUnit.Hour))

# ── Bar feed ──────────────────────────────────────────────────────────────────
class AlpacaBarFeed(BaseFeed):
    """
Polls Alpaca REST API once per bar close.
Emits the most recently completed bar for each symbol.
Also provides fetch_history() for the pipeline prefetch.
"""

    def __init__(self, symbols: list[str], timeframe: str = "1h") -> None:
        super().__init__(symbols, timeframe)
        self._tf_seconds  = _TF_MAP.get(timeframe, 3600)
        self._crypto_syms = [s for s in symbols if asset_class(s) == AssetClass.CRYPTO]
        self._stock_syms  = [s for s in symbols if asset_class(s) == AssetClass.EQUITY]
        self._crypto_client = None
        self._stock_client  = None

    @property
    def name(self) -> str:
        return "alpaca-bars"

    async def connect(self) -> None:
        from alpaca.data import CryptoHistoricalDataClient, StockHistoricalDataClient
        key, secret = settings.alpaca_api_key, settings.alpaca_secret_key
        self._crypto_client = CryptoHistoricalDataClient(key, secret)
        self._stock_client  = StockHistoricalDataClient(key, secret)
        logger.info(
            f"[alpaca-bars] connected | "
            f"tf={self.timeframe} ({self._tf_seconds}s) | "
            f"crypto={self._crypto_syms} equity={self._stock_syms}"
        )

    async def disconnect(self) -> None:
        self._crypto_client = None
        self._stock_client  = None

    def update_symbols(self, symbols: list[str]) -> None:
        """Update symbol list and re-partition into crypto/equity buckets."""
        self.symbols      = list(symbols)
        self._crypto_syms = [s for s in symbols if asset_class(s) == AssetClass.CRYPTO]
        self._stock_syms  = [s for s in symbols if asset_class(s) == AssetClass.EQUITY]

    def update_timeframe(self, timeframe: str) -> None:
        """Update timeframe and recalculate the bar-interval seconds."""
        self.timeframe   = timeframe
        self._tf_seconds = _TF_MAP.get(timeframe, 3600)

    async def _stream(self) -> None:
        """Wait for each bar close, then fetch and emit the latest bar."""
        while self._running:
            wait = _seconds_until_next_bar(self._tf_seconds)
            logger.debug(f"[alpaca-bars] next bar in {wait:.0f}s")

            try:
                await asyncio.sleep(wait)
            except asyncio.CancelledError:
                break

            # Fetch the latest completed bar for all symbols concurrently
            tasks = [
                asyncio.create_task(self._fetch_and_emit(sym))
                for sym in self.symbols
            ]
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _fetch_and_emit(self, symbol: str, *, _retries: int = 3) -> None:
        last_exc: Exception | None = None
        for attempt in range(_retries):
            try:
                bars = await self.fetch_history(symbol, n_bars=2)
                if bars:
                    await self._emit(bars[-1])
                    logger.debug(
                        f"[alpaca-bars] emitted {symbol} "
                        f"close={bars[-1].close:.4f}"
                    )
                return
            except Exception as exc:
                last_exc = exc
                if attempt < _retries - 1:
                    wait = 2 ** attempt   # 1s → 2s → 4s
                    logger.debug(
                        f"[alpaca-bars] {symbol} attempt {attempt+1}/{_retries} "
                        f"failed ({exc}), retrying in {wait}s"
                    )
                    await asyncio.sleep(wait)
        logger.warning(f"[alpaca-bars] fetch failed for {symbol} after {_retries} attempts: {last_exc}")

    async def fetch_history(self, symbol: str, n_bars: int = 512) -> list[OHLCVBar]:
        """
Fetch the last `n_bars` completed bars via REST.
Used by the pipeline prefetch AND by _fetch_and_emit for live bars.
        """
        from alpaca.data import CryptoHistoricalDataClient, StockHistoricalDataClient
        from alpaca.data.requests import CryptoBarsRequest, StockBarsRequest

        key, secret = settings.alpaca_api_key, settings.alpaca_secret_key

        if self._crypto_client is None:
            self._crypto_client = CryptoHistoricalDataClient(key, secret)
        if self._stock_client is None:
            self._stock_client = StockHistoricalDataClient(key, secret)

        loop       = asyncio.get_running_loop()
        alpaca_sym = to_alpaca_symbol(symbol)
        alpaca_tf  = _alpaca_timeframe(self.timeframe)

        # Start far enough back to guarantee n_bars of completed candles
        tf_hours = self._tf_seconds / 3600
        start    = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=tf_hours * n_bars * 1.5)

        if asset_class(symbol) == AssetClass.CRYPTO:
            req = CryptoBarsRequest(
                symbol_or_symbols=alpaca_sym,
                timeframe=alpaca_tf,
                limit=n_bars,
                start=start,
            )
            df = await loop.run_in_executor(
                None, lambda: self._crypto_client.get_crypto_bars(req).df
            )
        else:
            req = StockBarsRequest(
                symbol_or_symbols=alpaca_sym,
                timeframe=alpaca_tf,
                limit=n_bars,
                start=start,
            )
            df = await loop.run_in_executor(
                None, lambda: self._stock_client.get_stock_bars(req).df
            )

        result: list[OHLCVBar] = []
        for ts, row in df.iterrows():
            if isinstance(ts, tuple): ts = ts[1]
            result.append(OHLCVBar(
                symbol=symbol,
                timestamp=pd.Timestamp(ts).to_pydatetime().replace(tzinfo=timezone.utc),
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=float(row.get("volume", 0)),
                timeframe=self.timeframe,
            ))
        return result


# ── Price feed ────────────────────────────────────────────────────────────────
class AlpacaPriceFeed:
    """
Polls current mid-price for all tracked symbols every `poll_interval` seconds.
Used by the crypto position monitor to check SL/TP intra-bar.
Not a BaseFeed subclass — doesn't emit bars, just maintains a price dict.
"""

    def __init__(self, symbols: list[str], poll_interval: float = 10.0) -> None:
        self.symbols       = symbols
        self.poll_interval = poll_interval
        self._prices:  dict[str, float] = {}
        self._running  = False
        self._task: asyncio.Task | None = None
        self._crypto_client = None
        self._stock_client  = None

    async def start(self) -> None:
        from alpaca.data import CryptoHistoricalDataClient, StockHistoricalDataClient
        key, secret = settings.alpaca_api_key, settings.alpaca_secret_key
        self._crypto_client = CryptoHistoricalDataClient(key, secret)
        self._stock_client  = StockHistoricalDataClient(key, secret)
        self._running = True
        self._task    = asyncio.create_task(self._poll_loop(), name="price-feed")
        logger.info(
            f"[alpaca-price] started | "
            f"symbols={self.symbols} interval={self.poll_interval}s"
        )

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        logger.info("[alpaca-price] stopped")

    def latest(self, symbol: str) -> float | None:
        """Return the most recently fetched mid-price, or None."""
        return self._prices.get(symbol)

    async def _poll_loop(self) -> None:
        while self._running:
            try:
                await asyncio.gather(
                    *[self._fetch_price(s) for s in self.symbols],
                    return_exceptions=True,
                )
            except Exception as exc:
                logger.debug(f"[alpaca-price] poll error: {exc}")
            try:
                await asyncio.sleep(self.poll_interval)
            except asyncio.CancelledError:
                break

    async def _fetch_price(self, symbol: str) -> None:
        from alpaca.data.requests import (
            CryptoLatestQuoteRequest,
            StockLatestQuoteRequest,
        )
        loop       = asyncio.get_running_loop()
        alpaca_sym = to_alpaca_symbol(symbol)

        try:
            if asset_class(symbol) == AssetClass.CRYPTO:
                req    = CryptoLatestQuoteRequest(symbol_or_symbols=alpaca_sym)
                result = await loop.run_in_executor(
                    None, lambda: self._crypto_client.get_crypto_latest_quote(req)
                )
                quote = result.get(alpaca_sym)
            else:
                req    = StockLatestQuoteRequest(symbol_or_symbols=alpaca_sym)
                result = await loop.run_in_executor(
                    None, lambda: self._stock_client.get_stock_latest_quote(req)
                )
                quote = result.get(alpaca_sym)

            if quote and quote.ask_price and quote.bid_price:
                mid = (float(quote.ask_price) + float(quote.bid_price)) / 2
                self._prices[symbol] = mid
                logger.debug(f"[alpaca-price] {symbol} mid={mid:.4f}")

        except Exception as exc:
            logger.debug(f"[alpaca-price] {symbol} quote error: {exc}")


class AlpacaFeed(BaseFeed):
    """
    Live bar feed via Alpaca WebSocket.
    Splits symbols across CryptoDataStream and StockDataStream automatically.
    """

    def __init__(self, symbols: list[str], timeframe: str = "1h") -> None:
        super().__init__(symbols, timeframe)
        self._crypto_syms = [s for s in symbols if asset_class(s) == AssetClass.CRYPTO]
        self._stock_syms  = [s for s in symbols if asset_class(s) == AssetClass.EQUITY]

    @property
    def name(self) -> str: return "alpaca"

    async def connect(self) -> None: pass

    async def disconnect(self) -> None: pass

    async def _stream(self) -> None:
        tasks = []
        if self._crypto_syms: tasks.append(asyncio.create_task(self._stream_crypto()))
        if self._stock_syms: tasks.append(asyncio.create_task(self._stream_stocks()))

        if not tasks:
            while self._running: await asyncio.sleep(1)
            return

        try:
            await asyncio.gather(*tasks)
        except Exception:
            for t in tasks: t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

    # ── Crypto stream ─────────────────────────────────────────────────────────
    async def _stream_crypto(self) -> None:
        syms = [to_alpaca_symbol(s) for s in self._crypto_syms]
        logger.debug(f"[alpaca-crypto] connecting | symbols={syms}")

        async with websockets.connect(_CRYPTO_WS) as ws:
            await ws.send(json.dumps({
                "action": "auth",
                "key": settings.alpaca_api_key,
                "secret": settings.alpaca_secret_key
            }))
            resp = json.loads(await ws.recv())
            if not any(m.get("msg") == "connected" for m in resp):
                raise RuntimeError(f"[alpaca-crypto] auth failed: {resp}")

            await ws.send(json.dumps({"action": "subscribe", "bars": syms}))
            sub = json.loads(await ws.recv())
            logger.info(f"[alpaca-crypto] subscribed | {sub}")

            async for raw in ws:
                if not self._running: break
                for msg in json.loads(raw):
                    if msg.get("T") == "b": await self._emit_bar(msg)

    # ── Stock stream ──────────────────────────────────────────────────────────
    async def _stream_stocks(self) -> None:
        feed = "iex" if settings.alpaca_paper else "sip"
        url  = _STOCK_WS.format(feed=feed)
        logger.debug(f"[alpaca-stocks] connecting | symbols={self._stock_syms}")

        async with websockets.connect(url) as ws:
            await ws.send(json.dumps({
                "action": "auth",
                "key": settings.alpaca_api_key,
                "secret": settings.alpaca_secret_key
            }))
            resp = json.loads(await ws.recv())
            if not any(m.get("msg") == "connected" for m in resp):
                raise RuntimeError(f"[alpaca-stocks] auth failed: {resp}")

            await ws.send(json.dumps({"action": "subscribe", "bars": self._stock_syms}))
            sub = json.loads(await ws.recv())
            logger.info(f"[alpaca-stocks] subscribed | {sub}")

            async for raw in ws:
                if not self._running: break
                for msg in json.loads(raw):
                    if msg.get("T") == "b": await self._emit_bar(msg)

    # ── Shared bar handler ────────────────────────────────────────────────────
    async def _emit_bar(self, msg: dict) -> None:
        """Convert raw Alpaca bar message into OHLCVBar and push to queue."""
        try:
            symbol = from_alpaca_symbol(msg["S"])

            ts = datetime.fromisoformat(msg["t"].replace("Z", "+00:00"))

            bar = OHLCVBar(
                symbol=symbol,
                timestamp=ts,
                open=float(msg["o"]),
                high=float(msg["h"]),
                low=float(msg["l"]),
                close=float(msg["c"]),
                volume=float(msg.get("v", 0)),
                timeframe=self.timeframe
            )
            await self._emit(bar)
            logger.debug(f"[alpaca] bar {symbol} close={bar.close}")

        except Exception as exc:
            logger.warning(f"[alpaca] failed to parse bar msg: {exc} | raw={msg}")

    # ── Historical fetch ──────────────────────────────────────────────────────
    async def fetch_history(self, symbol: str, n_bars: int = 512) -> list[OHLCVBar]:
        from alpaca.data import CryptoHistoricalDataClient, StockHistoricalDataClient
        from alpaca.data.requests import CryptoBarsRequest, StockBarsRequest
        from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
        import pandas as pd

        key    = settings.alpaca_api_key
        secret = settings.alpaca_secret_key

        tf_map = {
            "1m":  TimeFrame(1,  TimeFrameUnit.Minute),
            "5m":  TimeFrame(5,  TimeFrameUnit.Minute),
            "15m": TimeFrame(15, TimeFrameUnit.Minute),
            "1h":  TimeFrame(1,  TimeFrameUnit.Hour),
            "1d":  TimeFrame(1,  TimeFrameUnit.Day),
        }

        tf_hours = {"1m": 1/60, "5m": 5/60, "15m": 15/60, "1h": 1, "4h": 4, "1d": 24}
        hours_needed = tf_hours.get(self.timeframe, 1) * n_bars
        start = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=hours_needed * 1.5)

        alpaca_tf  = tf_map.get(self.timeframe, TimeFrame(1, TimeFrameUnit.Hour))
        alpaca_sym = to_alpaca_symbol(symbol)
        loop       = asyncio.get_running_loop()

        if asset_class(symbol) == AssetClass.CRYPTO:
            client  = CryptoHistoricalDataClient(key, secret)
            req     = CryptoBarsRequest(
                symbol_or_symbols=alpaca_sym,
                timeframe=alpaca_tf,
                limit=n_bars,
                start=start
            )
            bars_df = await loop.run_in_executor(
                None, lambda: client.get_crypto_bars(req).df
            )
        else:
            client  = StockHistoricalDataClient(key, secret)
            req     = StockBarsRequest(
                symbol_or_symbols=alpaca_sym,
                timeframe=alpaca_tf,
                limit=n_bars,
                start=start
            )
            bars_df = await loop.run_in_executor(
                None, lambda: client.get_stock_bars(req).df
            )

        result: list[OHLCVBar]=  []
        for ts, row in bars_df.iterrows():
            if isinstance(ts, tuple): ts = ts[1]
            result.append(OHLCVBar(
                symbol=symbol,
                timestamp=pd.Timestamp(ts).to_pydatetime().replace(tzinfo=timezone.utc),
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=float(row.get("volume", 0)),
                timeframe=self.timeframe,
            ))

        return result

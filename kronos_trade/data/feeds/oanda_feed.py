"""
kronos_trade/data/feeds/oanda_feed.py

OANDA v20 market-data feeds — no extra SDK, pure httpx.

OANDABarFeed
  Polls the OANDA candles REST endpoint once per bar close and emits the
  most recently completed candle for each symbol.  Implements fetch_history()
  so the DataPipeline can seed Kronos' context window at startup.

OANDAPriceFeed
  Streams real-time bid/ask mid-prices via OANDA's SSE pricing stream
  (`/accounts/{id}/pricing/stream`).  Falls back to polling the REST
  snapshot endpoint if the stream drops.

Instrument format: KATS → "AUDCHF", OANDA → "AUD_CHF"
Granularity map  : KATS → OANDA  ("1m" → "M1", "1h" → "H1", …)
"""
from __future__ import annotations

import asyncio
import json
import time as _time
from datetime import datetime, timezone
from typing import Optional

import httpx
from loguru import logger

from kronos_trade.config import settings
from kronos_trade.models import OHLCVBar

from .base import BaseFeed


# ── Helpers ────────────────────────────────────────────────────────────────────

def _to_oanda(symbol: str) -> str:
    """AUDCHF → AUD_CHF"""
    s = symbol.replace("/", "").replace("-", "").replace("_", "").upper()
    return f"{s[:3]}_{s[3:]}" if len(s) == 6 else s


def _from_oanda(symbol: str) -> str:
    """AUD_CHF → AUDCHF"""
    return symbol.replace("_", "")


_TF_MAP: dict[str, str] = {
    "1m":  "M1",
    "5m":  "M5",
    "15m": "M15",
    "30m": "M30",
    "1h":  "H1",
    "4h":  "H4",
    "1d":  "D",
    "1w":  "W",
}

_TF_SECONDS: dict[str, int] = {
    "1m":  60,
    "5m":  300,
    "15m": 900,
    "30m": 1800,
    "1h":  3600,
    "4h":  14400,
    "1d":  86400,
    "1w":  604800,
}


def _seconds_until_next_bar(tf_seconds: int, offset: int = 5) -> float:
    """Seconds until the next bar close, plus a small buffer."""
    now       = _time.time()
    next_bar  = (int(now / tf_seconds) + 1) * tf_seconds
    return max(0.0, next_bar - now + offset)


def _build_headers() -> dict[str, str]:
    return {
        "Authorization":          f"Bearer {settings.oanda_api_token}",
        "Content-Type":           "application/json",
        "Accept-Datetime-Format": "RFC3339",
    }


def _rest_base() -> str:
    env = "fxpractice" if settings.oanda_practice else "fxtrade"
    return f"https://api-{env}.oanda.com/v3"


def _stream_base() -> str:
    env = "fxpractice" if settings.oanda_practice else "fxtrade"
    return f"https://stream-{env}.oanda.com/v3"


# ── Bar feed ───────────────────────────────────────────────────────────────────

class OANDABarFeed(BaseFeed):
    """
    Polls OANDA's candles endpoint once per bar-close and emits the
    latest completed candle for every tracked symbol.

    Poll timing mirrors AlpacaBarFeed: wait until wall-clock aligns with the
    next bar boundary (+ 5 s buffer), then fetch `count=2` and emit the
    penultimate (completed) candle.
    """

    def __init__(self, symbols: list[str], timeframe: str = "1h") -> None:
        super().__init__(symbols, timeframe)
        self._tf_seconds = _TF_SECONDS.get(timeframe, 3600)
        self._http: Optional[httpx.AsyncClient] = None

    @property
    def name(self) -> str:
        return "oanda-bars"

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def connect(self) -> None:
        self._http = httpx.AsyncClient(
            headers=_build_headers(),
            timeout=15.0,
        )
        env_label = "PRACTICE" if settings.oanda_practice else "LIVE"
        logger.info(f"[oanda-bars] connected [{env_label}] | tf={self.timeframe}")

    async def disconnect(self) -> None:
        if self._http:
            await self._http.aclose()
            self._http = None
        logger.info("[oanda-bars] disconnected")

    def update_timeframe(self, timeframe: str) -> None:
        self.timeframe   = timeframe
        self._tf_seconds = _TF_SECONDS.get(timeframe, 3600)

    # ── Stream loop ────────────────────────────────────────────────────────────

    async def _stream(self) -> None:
        """Sleep until next bar close, then fetch + emit for every symbol."""
        wait = _seconds_until_next_bar(self._tf_seconds)
        logger.debug(f"[oanda-bars] next bar in {wait:.0f}s")
        try:
            await asyncio.sleep(wait)
        except asyncio.CancelledError:
            return

        if not self._running:
            return

        for sym in list(self.symbols):
            try:
                bar = await self._fetch_last_closed_bar(sym)
                if bar:
                    await self._emit(bar)
            except Exception as exc:
                logger.warning(f"[oanda-bars] {sym} fetch error: {exc}")

    # ── Candle fetcher ─────────────────────────────────────────────────────────

    async def _fetch_last_closed_bar(self, symbol: str) -> OHLCVBar | None:
        instrument  = _to_oanda(symbol)
        granularity = _TF_MAP.get(self.timeframe, "H1")

        resp = await self._http.get(
            f"{_rest_base()}/instruments/{instrument}/candles",
            params={
                "count":       2,           # last 2 bars — take the completed one
                "granularity": granularity,
                "price":       "M",         # mid-price candles
            },
        )
        resp.raise_for_status()
        candles = resp.json().get("candles", [])

        # The second-to-last candle is the most recently *completed* bar
        completed = [c for c in candles if c.get("complete", False)]
        if not completed:
            return None

        c   = completed[-1]
        mid = c.get("mid", {})
        ts  = datetime.fromisoformat(c["time"].replace("Z", "+00:00"))

        return OHLCVBar(
            symbol    = symbol,
            timestamp = ts,
            open      = float(mid.get("o", 0)),
            high      = float(mid.get("h", 0)),
            low       = float(mid.get("l", 0)),
            close     = float(mid.get("c", 0)),
            volume    = float(c.get("volume", 0)),
            timeframe = self.timeframe,
        )

    # ── Historical fetch ───────────────────────────────────────────────────────

    async def fetch_history(self, symbol: str, n_bars: int = 512) -> list[OHLCVBar]:
        """Fetch up to `n_bars` completed candles for Kronos context seeding."""
        if not self._http:
            # Fetch_history may be called before connect(); create a one-shot client.
            async with httpx.AsyncClient(headers=_build_headers(), timeout=20.0) as client:
                return await self._do_fetch_history(client, symbol, n_bars)
        return await self._do_fetch_history(self._http, symbol, n_bars)

    async def _do_fetch_history(
        self,
        client: httpx.AsyncClient,
        symbol: str,
        n_bars: int,
    ) -> list[OHLCVBar]:
        instrument  = _to_oanda(symbol)
        granularity = _TF_MAP.get(self.timeframe, "H1")

        try:
            resp = await client.get(
                f"{_rest_base()}/instruments/{instrument}/candles",
                params={
                    "count":       min(n_bars, 5000),   # OANDA max per request
                    "granularity": granularity,
                    "price":       "M",
                },
            )
            resp.raise_for_status()
        except Exception as exc:
            logger.error(f"[oanda-bars] fetch_history {symbol}: {exc}")
            return []

        bars: list[OHLCVBar] = []
        for c in resp.json().get("candles", []):
            if not c.get("complete", True):   # skip the in-progress bar
                continue
            mid = c.get("mid", {})
            ts  = datetime.fromisoformat(c["time"].replace("Z", "+00:00"))
            bars.append(OHLCVBar(
                symbol    = symbol,
                timestamp = ts,
                open      = float(mid.get("o", 0)),
                high      = float(mid.get("h", 0)),
                low       = float(mid.get("l", 0)),
                close     = float(mid.get("c", 0)),
                volume    = float(c.get("volume", 0)),
                timeframe = self.timeframe,
            ))
        logger.debug(f"[oanda-bars] history {symbol}: {len(bars)} bars")
        return bars


# ── Price feed (SSE streaming) ─────────────────────────────────────────────────

class OANDAPriceFeed:
    """
    Real-time bid/ask mid-prices via OANDA's SSE pricing stream.

    The stream endpoint (`/accounts/{id}/pricing/stream`) sends newline-
    delimited JSON events — either `PRICE` ticks or `HEARTBEAT` keepalives.
    On disconnect the feed reconnects automatically.

    Interface mirrors AlpacaPriceFeed:
      price_feed.latest("AUDCHF")  → float | None
      price_feed.start() / .stop()
    """

    def __init__(
        self,
        symbols: list[str],
        poll_interval: float = 2.0,   # reconnect delay on stream failure
    ) -> None:
        self.symbols       = list(symbols)
        self.poll_interval = poll_interval
        self._prices:  dict[str, float] = {}
        self._running  = False
        self._task: asyncio.Task | None = None

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def start(self) -> None:
        self._running = True
        self._task    = asyncio.create_task(self._stream_loop(), name="oanda-price-stream")
        env_label     = "PRACTICE" if settings.oanda_practice else "LIVE"
        logger.info(
            f"[oanda-price] started [{env_label}] | symbols={self.symbols}"
        )

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        logger.info("[oanda-price] stopped")

    def latest(self, symbol: str) -> float | None:
        """Return the most recently received mid-price, or None."""
        return self._prices.get(symbol)

    def update_symbols(self, symbols: list[str]) -> None:
        """Replace tracked symbols; the stream loop will reconnect on next iteration."""
        self.symbols = list(symbols)

    # ── Internal ──────────────────────────────────────────────────────────────

    async def _stream_loop(self) -> None:
        while self._running:
            try:
                await self._open_stream()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning(
                    f"[oanda-price] stream error: {exc} — reconnecting in {self.poll_interval}s"
                )
            try:
                await asyncio.sleep(self.poll_interval)
            except asyncio.CancelledError:
                break

    async def _open_stream(self) -> None:
        if not self.symbols:
            await asyncio.sleep(1)
            return

        instruments = ",".join(_to_oanda(s) for s in self.symbols)
        url = (
            f"{_stream_base()}/accounts/{settings.oanda_account_id}"
            f"/pricing/stream?instruments={instruments}"
        )

        async with httpx.AsyncClient(
            headers=_build_headers(),
            timeout=httpx.Timeout(connect=10.0, read=None, write=10.0, pool=10.0),
        ) as client:
            async with client.stream("GET", url) as resp:
                resp.raise_for_status()
                logger.debug("[oanda-price] SSE stream open")

                async for raw_line in resp.aiter_lines():
                    if not self._running:
                        return
                    if not raw_line.strip():
                        continue
                    try:
                        msg = json.loads(raw_line)
                    except json.JSONDecodeError:
                        continue

                    msg_type = msg.get("type")
                    if msg_type == "PRICE":
                        self._handle_price_tick(msg)
                    elif msg_type == "HEARTBEAT":
                        logger.trace("[oanda-price] heartbeat")

    def _handle_price_tick(self, msg: dict) -> None:
        oanda_sym = msg.get("instrument", "")
        symbol    = _from_oanda(oanda_sym)

        bids = msg.get("bids", [])
        asks = msg.get("asks", [])
        if not bids or not asks:
            return

        bid = float(bids[0].get("price", 0))
        ask = float(asks[0].get("price", 0))
        if bid and ask:
            mid                   = (bid + ask) / 2
            self._prices[symbol]  = mid
            logger.trace(f"[oanda-price] {symbol} mid={mid:.5f}")

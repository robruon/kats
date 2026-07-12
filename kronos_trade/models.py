"""
Core domain models shared across all layers.
"""
from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


# ── Enums ─────────────────────────────────────────────────────────────────────

class Direction(str, Enum):
    LONG  = "long"
    SHORT = "short"
    FLAT  = "flat"


class OrderSide(str, Enum):
    BUY  = "buy"
    SELL = "sell"


class OrderStatus(str, Enum):
    PENDING   = "pending"
    SUBMITTED = "submitted"
    FILLED    = "filled"
    CANCELLED = "cancelled"
    REJECTED  = "rejected"


class BrokerName(str, Enum):
    ALPACA       = "alpaca"
    OANDA        = "oanda"
    NINJATRADER  = "ninjatrader"
    IBKR         = "ibkr"
    CTRADER      = "ctrader"
    PAPER        = "paper"


# ── OHLCV bar ─────────────────────────────────────────────────────────────────

class OHLCVBar(BaseModel):
    symbol:    str
    timestamp: datetime
    open:      float
    high:      float
    low:       float
    close:     float
    volume:    float = 0.0
    amount:    float = 0.0          # notional traded (optional, used by Kronos)
    timeframe: str = "1h"


# ── Kronos signal ─────────────────────────────────────────────────────────────

class KronosSignal(BaseModel):
    symbol:              str
    generated_at:        datetime
    timeframe:           str
    direction:           Direction
    confidence:          float = Field(..., ge=0.0, le=1.0)   # P(price higher)
    forecast_mean:       list[float]                           # predicted close prices
    forecast_lower:      list[float]                           # lower uncertainty band
    forecast_upper:      list[float]                           # upper uncertainty band
    forecast_timestamps: list[datetime]
    volatility_forecast: float                                 # predicted ATR equivalent
    entry_price:         float
    atr:                 float                                 # current ATR for sizing


# ── Order / position ──────────────────────────────────────────────────────────

class EntryDetail(BaseModel):
    """Tracks one individual entry within a scaled position."""
    order_id:     str   = ""
    direction:    str   = "long"
    quantity:     float = 0.0
    entry_price:  float = 0.0
    stop_loss:    float = 0.0
    take_profit:  float = 0.0
    submitted_at: float = 0.0
    tp_order_id:  str   = ""   # broker-side take-profit limit order
    sl_order_id:  str   = ""   # broker-side stop-loss stop-limit order


class BracketOrder(BaseModel):
    symbol:       str
    side:         OrderSide
    quantity:     float
    entry_price:  float
    stop_loss:    float
    take_profit:  float
    broker:       BrokerName
    signal_id:    Optional[str] = None
    created_at:   datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    status:       OrderStatus = OrderStatus.PENDING
    broker_order_id: Optional[str] = None
    filled_price: Optional[float] = None
    filled_at:    Optional[datetime] = None
    reject_reason: str = ""


class Position(BaseModel):
    symbol:       str
    direction:    Direction
    quantity:     float
    entry_price:  float
    current_price: float
    stop_loss:    float
    take_profit:  float
    broker:       BrokerName
    opened_at:    datetime
    unrealized_pnl: float = 0.0
    entries:        list[EntryDetail] = Field(default_factory=list)
    broker_order_id: Optional[str] = None

    @property
    def side(self) -> OrderSide:
        return OrderSide.BUY if self.direction == Direction.LONG else OrderSide.SELL

    def update_pnl(self, price: float) -> None:
        self.current_price = price
        multiplier = 1.0 if self.direction == Direction.LONG else -1.0
        self.unrealized_pnl = multiplier * (price - self.entry_price) * self.quantity


# ── Account snapshot ─────────────────────────────────────────────────────────

class AccountSnapshot(BaseModel):
    broker:          BrokerName
    equity:          float
    cash:            float
    buying_power:    float
    daily_pnl:       float = 0.0
    unrealized_pnl:  float = 0.0
    timestamp:       datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


# ── System state ─────────────────────────────────────────────────────────────

class SystemState(BaseModel):
    running:           bool = False
    kronos_loaded:     bool = False
    kronos_paused:     bool = False
    active_feeds:      list[str] = Field(default_factory=list)
    active_brokers:    list[str] = Field(default_factory=list)
    positions:         list[Position] = Field(default_factory=list)
    daily_pnl:         float = 0.0
    daily_loss_halted: bool = False
    drawdown_halted:   bool = False
    kill_switch:       bool = False
    last_signal:       Optional[KronosSignal] = None
    uptime_seconds:    float = 0.0
    trading_mode:      str = "day"
    timeframe:         str = "1h"

class AssetClass(str, Enum):
    CRYPTO  = "crypto"
    EQUITY  = "equity"
    FUTURES = "futures"
    FOREX   = "forex"

# ── Symbol normalisation ───────────────────────────────────────────────────────

# Runtime registry: internal symbol → Alpaca slash format.
# e.g. "SOLUSD" → "SOL/USD",  "ETHBTC" → "ETH/BTC",  "BTCUSDT" → "BTC/USDT"
#
# This dict is populated dynamically by AlpacaAdapter._fetch_tradeable_sync()
# at broker connect time so it covers every pair Alpaca actually supports
# (including USDC/USDT/BTC quote currencies) without any hardcoding.
#
# Bootstrap entries let the system work before the first broker connect
# (e.g. during config validation or unit tests).
_ALPACA_CRYPTO_SLASH: dict[str, str] = {
    "BTCUSD": "BTC/USD",
    "ETHUSD": "ETH/USD",
    "SOLUSD": "SOL/USD",
}


def register_crypto_symbol(internal: str, alpaca_format: str) -> None:
    """
    Register a live Alpaca crypto symbol mapping.
    Called by AlpacaAdapter for every tradeable crypto asset it discovers.
    Safe to call multiple times with the same pair.
    """
    _ALPACA_CRYPTO_SLASH[internal] = alpaca_format


# Non-crypto symbols with explicit asset-class overrides.
# Equities, futures, and forex are never auto-detected — they must be listed here.
_SYMBOL_ASSET_CLASS_EXPLICIT: dict[str, AssetClass] = {
    # Equities
    "SPY":    AssetClass.EQUITY,
    "QQQ":    AssetClass.EQUITY,
    "AAPL":   AssetClass.EQUITY,
    "TSLA":   AssetClass.EQUITY,
    "NVDA":   AssetClass.EQUITY,
    "MSFT":   AssetClass.EQUITY,
    "AMZN":   AssetClass.EQUITY,
    "GOOGL":  AssetClass.EQUITY,
    # Futures
    "NQ":     AssetClass.FUTURES,
    "MNQ":    AssetClass.FUTURES,
    "ES":     AssetClass.FUTURES,
    "MES":    AssetClass.FUTURES,
    "CL":     AssetClass.FUTURES,
    "GC":     AssetClass.FUTURES,
    # Forex / metals
    "XAUUSD": AssetClass.FOREX,
    "AUDJPY": AssetClass.FOREX,
    "EURUSD": AssetClass.FOREX,
    "GBPUSD": AssetClass.FOREX,
}


def asset_class(symbol: str) -> AssetClass | None:
    """
    Return the AssetClass for a symbol.

    Resolution order:
      1. Explicit non-crypto overrides (equities, futures, forex).
      2. Runtime crypto registry (_ALPACA_CRYPTO_SLASH) — populated from the
         live Alpaca asset list at connect time, so any pair Alpaca supports
         (BTC/USD, ETH/BTC, SOL/USDT …) is covered automatically.
      3. Unknown → None.
    """
    if symbol in _SYMBOL_ASSET_CLASS_EXPLICIT:
        return _SYMBOL_ASSET_CLASS_EXPLICIT[symbol]
    if symbol in _ALPACA_CRYPTO_SLASH:
        return AssetClass.CRYPTO
    return None


# Legacy alias kept for any code that imports SYMBOL_ASSET_CLASS directly.
# Reads from the same live dicts so it stays current after broker connect.
class _SymbolAssetClassProxy:
    def get(self, key: str, default=None):
        return asset_class(key) or default
    def __contains__(self, key: str) -> bool:
        return asset_class(key) is not None
    def __getitem__(self, key: str):
        v = asset_class(key)
        if v is None: raise KeyError(key)
        return v

SYMBOL_ASSET_CLASS = _SymbolAssetClassProxy()


def to_alpaca_symbol(symbol: str) -> str:
    """
    Convert an internal symbol to the format Alpaca's API expects.
    Registered crypto pairs use their exact Alpaca slash format (e.g. SOL/USD,
    ETH/BTC, BTC/USDT).  Everything else passes through unchanged.
    """
    return _ALPACA_CRYPTO_SLASH.get(symbol, symbol)


def from_alpaca_symbol(symbol: str) -> str:
    """Convert Alpaca's slash format back to our internal format (SOL/USD → SOLUSD)."""
    return symbol.replace("/", "")
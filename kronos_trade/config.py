"""
Central configuration — all env vars flow through here.
"""
from __future__ import annotations

import os
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import Literal

import json as _json

from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class TradingMode(str, Enum):
    SCALPING = "scalping"
    DAY      = "day"
    SWING    = "swing"


# Per-mode multiplier tables (confidence_min, sl_mult, tp_mult, size_scale)
TRADING_MODE_PARAMS: dict[TradingMode, dict] = {
    TradingMode.SCALPING: {"confidence_min": 0.80, "sl_mult": 0.5, "tp_mult": 1.0, "size_scale": 0.5},
    TradingMode.DAY:      {"confidence_min": 0.65, "sl_mult": 1.0, "tp_mult": 2.0, "size_scale": 1.0},
    TradingMode.SWING:    {"confidence_min": 0.55, "sl_mult": 2.0, "tp_mult": 4.0, "size_scale": 0.75},
}


class KronosSettings(BaseSettings):
    model_size: Literal["mini", "small", "base"] = "small"
    device: str = "cuda"
    max_context: int = 512
    forecast_horizon: int = 24       # bars ahead
    mc_samples: int = 50             # Monte Carlo draws for uncertainty bands

    @property
    def hf_model_id(self) -> str:
        return f"NeoQuasar/Kronos-{self.model_size}"

    @property
    def hf_tokenizer_id(self) -> str:
        return "NeoQuasar/Kronos-Tokenizer-base"


class FeedSettings(BaseSettings):
    databento_api_key: str = ""
    alpaca_api_key: str = ""
    alpaca_secret_key: str = ""
    alpaca_paper: bool = True


class RiskSettings(BaseSettings):
    max_daily_loss_pct: float = 2.0
    max_drawdown_pct: float = 5.0
    max_position_risk_pct: float = 1.0
    max_concurrent_positions: int = 4
    risk_free_rate: float = 0.05


class StrategySettings(BaseSettings):
    min_signal_confidence: float = Field(0.60, ge=0.0, le=1.0)
    position_sizing: Literal["fixed", "volatility", "kelly"] = "volatility"
    default_rr_ratio: float = Field(2.0, ge=1.0)


class NTSettings(BaseSettings):
    nt8_webhook_host: str = "localhost"
    nt8_webhook_port: int = 8080
    nt8_account_id: str = "Sim101"

_PROJECT_ROOT = Path(__file__).parents[1]
_ENV_FILE     = _PROJECT_ROOT / ".env"

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(_ENV_FILE),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Sub-groups (flat env vars, no nesting needed) ──────────────────────
    # Kronos
    kronos_model_size: Literal["mini", "small", "base"] = "small"
    kronos_device: str = "cuda"
    kronos_max_context: int = 512
    kronos_forecast_horizon: int = 24
    kronos_mc_samples: int = 50

    # Feeds
    databento_api_key: str = ""
    alpaca_api_key: str = ""
    alpaca_secret_key: str = ""
    alpaca_paper: bool = True

    # Redis / DB
    redis_url: str = "redis://localhost:6379/0"
    database_url: str = "sqlite+aiosqlite:///./kronos_trade.db"

    # API
    api_host: str = "127.0.0.1"
    api_port: int = 8765
    api_secret: str = "change_me"

    # OANDA
    oanda_api_token:  str = ""
    oanda_account_id: str = ""
    oanda_practice:   bool = True   # True = fxTrade Practice, False = fxTrade Live

    # NinjaTrader
    nt8_webhook_host: str = "localhost"
    nt8_webhook_port: int = 8080
    nt8_account_id: str = "Sim101"

    # Logging
    log_level: str = "INFO"
    log_file: str = "logs/kronos_trade.log"

    hf_token: str = ""
    hf_home: str = ""
    hf_hub_cache: str = ""
    torch_home: str = ""
    transformers_cache: str = ""

    def apply_cache_env(self) -> None:
        """Push cache paths into os.environ so HF/Torch libraries see them."""
        mappings = {
            "HF_HOME":            self.hf_home,
            "HF_HUB_CACHE":       self.hf_hub_cache,
            "TORCH_HOME":         self.torch_home,
            "TRANSFORMERS_CACHE": self.transformers_cache,
        }

        for key, val in mappings.items():
            if val:
                os.environ[key] = val

    @field_validator("kronos_device")
    @classmethod
    def validate_device(cls, v: str) -> str:
        import torch
        if v == "cuda" and not torch.cuda.is_available():
            return "cpu"
        if v == "mps":
            try:
                if not torch.backends.mps.is_available():
                    return "cpu"
            except AttributeError:
                return "cpu"
        return v

    @property
    def api_url(self) -> str:
        """Client-facing URL - maps 0.0.0.0 bind address to 127.0.0.1"""
        host = "127.0.0.1" if self.api_host == "0.0.0.0" else self.api_host
        return f"http://{host}:{self.api_port}"

    @property
    def api_ws_url(self) -> str:
        host = "127.0.0.1" if self.api_host == "0.0.0.0" else self.api_host
        return f"ws://{host}:{self.api_port}/ws"

    @property
    def kronos_hf_model_id(self) -> str:
        return f"NeoQuasar/Kronos-{self.kronos_model_size}"

    @property
    def kronos_hf_tokenizer_id(self) -> str:
        return "NeoQuasar/Kronos-Tokenizer-base"

    @property
    def redis_available(self) -> bool:
        """Check if Redis is reachable without hard-failing if it's not"""
        try:
            import redis as redis_lib
            r = redis_lib.from_url(self.redis_url, socket_connect_timeout=1)
            r.ping()
            return True
        except Exception:
            return False

@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


# Convenience alias
settings = get_settings()

settings.apply_cache_env()


# ── Runtime config (kats_config.json) ────────────────────────────────────────
# All non-secret, operator-facing config lives here. Written back on any
# runtime change (mode switch, timeframe switch, risk param update) so
# kats_config.json is always the authoritative source on restart — no
# .env baseline-tracking hacks needed.

_KATS_CFG_FILE = _PROJECT_ROOT / "kats_config.json"


class KatsConfig(BaseModel):
    # Instruments & timing
    instruments:              str   = "BTCUSD,ETHUSD"
    default_timeframe:        str   = "1h"
    trading_mode:             TradingMode = TradingMode.DAY

    # Risk
    max_daily_loss_pct:       float = 2.0
    max_drawdown_pct:         float = 5.0
    max_position_risk_pct:    float = 1.0
    max_concurrent_positions: int   = 4
    risk_free_rate:           float = 0.05
    allow_position_scaling:   bool  = True
    max_entries_per_symbol:   int   = 3
    max_position_pct:         float = 0.20

    # Strategy
    min_signal_confidence:    float = 0.60
    position_sizing:          str   = "volatility"
    default_rr_ratio:         float = 2.0

    # SL/TP multiplier overrides (applied on top of trading_mode defaults).
    # When set, these take precedence over TRADING_MODE_PARAMS so you can
    # tune risk/reward without changing the mode.
    # sl_mult: ATR multiple for stop-loss distance (e.g. 1.5 → SL = 1.5×ATR)
    # tp_mult: ATR multiple for take-profit distance (e.g. 3.0 → TP = 3.0×ATR)
    # Set to null / omit to use the mode defaults.
    sl_mult_override:         float | None = None
    tp_mult_override:         float | None = None

    # Trading schedule — restrict signal generation to specific days/hours (UTC).
    # Format: "DAYS:STARTEND"  where DAYS ⊆ 1234567 (1=Mon…7=Sun),
    #         START and END are 4-digit HHMM strings.
    # "1234567:00000000" or null → always active (no restriction).
    # Example: "12345:09001700" → weekdays 09:00–17:00 UTC only.
    trading_schedule:         str | None = None

    # OANDA sizing  (1,000 units per $1,000 = micro-lot equivalent)
    oanda_units_per_k:        int   = 1000

    # Kronos model (runtime-mutable — takes effect on next engine reload)
    kronos_model_size:        str   = "small"   # mini | small | base
    kronos_device:            str   = "cpu"     # cpu | mps | cuda
    kronos_max_context:       int   = 512
    kronos_forecast_horizon:  int   = 24
    kronos_mc_samples:        int   = 50

    @property
    def kronos_hf_model_id(self) -> str:
        return f"NeoQuasar/Kronos-{self.kronos_model_size}"

    @property
    def instrument_list(self) -> list[str]:
        return [i.strip() for i in self.instruments.split(",") if i.strip()]

    @classmethod
    def load(cls) -> "KatsConfig":
        if not _KATS_CFG_FILE.exists():
            return cls._migrate_from_settings()
        try:
            return cls.model_validate(_json.loads(_KATS_CFG_FILE.read_text()))
        except Exception:
            return cls._migrate_from_settings()

    def save(self) -> None:
        _KATS_CFG_FILE.write_text(self.model_dump_json(indent=2))

    @classmethod
    def _migrate_from_settings(cls) -> "KatsConfig":
        """First-run bootstrap: write kats_config.json with built-in defaults."""
        inst = cls()
        inst.save()
        import logging as _logging
        _logging.getLogger(__name__).info(
            "[config] kats_config.json created with defaults — edit via TUI or directly"
        )
        return inst


@lru_cache(maxsize=1)
def get_kats_config() -> KatsConfig:
    return KatsConfig.load()


kats_cfg = get_kats_config()

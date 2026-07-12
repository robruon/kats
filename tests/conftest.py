"""Shared pytest fixtures."""
import pytest

# Ensure settings use test defaults
import os
os.environ.setdefault("MAX_DAILY_LOSS_PCT", "2.0")
os.environ.setdefault("MAX_DRAWDOWN_PCT", "5.0")
os.environ.setdefault("MAX_POSITION_RISK_PCT", "1.0")
os.environ.setdefault("MAX_CONCURRENT_POSITIONS", "4")
os.environ.setdefault("MIN_SIGNAL_CONFIDENCE", "0.60")
os.environ.setdefault("DEFAULT_RR_RATIO", "2.0")
os.environ.setdefault("POSITION_SIZING", "volatility")

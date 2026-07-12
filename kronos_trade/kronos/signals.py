"""
kronos_trade/kronos/signals.py
Converts raw Kronos forecast arrays into KronosSignal objects.

Signal logic:
  - direction: compare forecast mean vs current price
  - confidence: fraction of MC samples that agree with direction
  - Only emits signal if confidence >= settings.min_signal_confidence
"""
from __future__ import annotations

from datetime import datetime, timezone

import numpy as np
from loguru import logger

from kronos_trade.config import kats_cfg
from kronos_trade.data.pipeline import BarHistory, DataPipeline
from kronos_trade.models import Direction, KronosSignal, AssetClass, asset_class as get_asset_class

from .predictor import KronosEngine


class SignalGenerator:
    """
    Runs after each new bar for each subscribed symbol.
    Calls KronosEngine.predict(), interprets the forecast, and returns a KronosSignal.
    """

    def __init__(self, engine: KronosEngine, pipeline: DataPipeline) -> None:
        self.engine   = engine
        self.pipeline = pipeline

    async def generate(self, symbol: str, timeframe: str) -> KronosSignal | None:
        """
        Generate a signal for `symbol`. Returns None if:
        - Not enough history
        - Confidence below threshold
        - Model not loaded
        """
        history = self.pipeline.history(symbol)

        if len(history) < 50:
            logger.debug(f"[signals] {symbol}: insufficient history ({len(history)}/50 bars)")
            return None

        df, x_ts = history.to_kronos_df()

        try:
            forecast = await self.engine.predict(df, x_ts, symbol=symbol)
        except Exception as exc:
            logger.error(f"[signals] {symbol}: forecast failed — {exc}")
            return None

        mean_fc  = forecast["mean"]       # (horizon,)
        lower_fc = forecast["lower"]
        upper_fc = forecast["upper"]
        samples  = forecast["samples"]    # (n_samples, horizon)
        ts_list  = forecast["timestamps"]

        current_price = history.last_close()
        if current_price is None:
            return None

        atr = history.atr(period=14)

        # ── Direction: compare horizon midpoint vs current price ──────────────
        # Use the midpoint of the forecast horizon for a balanced view
        mid = len(mean_fc) // 2
        forecast_target = float(mean_fc[mid])

        price_change_pct = (forecast_target - current_price) / current_price

        # ── Confidence: fraction of MC samples that agree ─────────────────────
        # "agree" = most samples predict price higher/lower than current
        bullish_frac = float(np.mean(samples[:, mid] > current_price))
        bearish_frac = 1.0 - bullish_frac

        if price_change_pct > 0:
            direction  = Direction.LONG
            confidence = bullish_frac
        elif price_change_pct < 0:
            direction  = Direction.SHORT
            confidence = bearish_frac
        else:
            direction  = Direction.FLAT
            confidence = 0.0

        if confidence < kats_cfg.min_signal_confidence:
            logger.debug(
                f"[signals] {symbol}: confidence {confidence:.2%} < "
                f"threshold {kats_cfg.min_signal_confidence:.2%} → FLAT"
            )
            return None

        # ── Volatility forecast: spread of 90th-10th pct at end of horizon ───
        vol_forecast = float(np.mean(upper_fc - lower_fc))

        signal = KronosSignal(
            symbol=symbol,
            generated_at=datetime.now(tz=timezone.utc),
            timeframe=timeframe,
            direction=direction,
            confidence=confidence,
            forecast_mean=mean_fc.tolist(),
            forecast_lower=lower_fc.tolist(),
            forecast_upper=upper_fc.tolist(),
            forecast_timestamps=ts_list,
            volatility_forecast=vol_forecast,
            entry_price=current_price,
            atr=atr,
        )

        logger.info(
            f"[signals] {symbol} {direction.value.upper()} "
            f"conf={confidence:.1%} vol={vol_forecast:.4f} "
            f"target={forecast_target:.4f}"
        )
        return signal

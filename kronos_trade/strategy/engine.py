"""
kronos_trade/strategy/engine.py
Converts a KronosSignal into a sized BracketOrder.

Position sizing modes:
  - fixed:      constant risk per trade (max_position_risk_pct of account)
  - volatility: ATR-based sizing (risk = ATR × multiplier)
  - kelly:      fractional Kelly criterion using Kronos confidence
"""
from __future__ import annotations

from loguru import logger

from kronos_trade.config import TRADING_MODE_PARAMS, TradingMode, kats_cfg
from kronos_trade.models import (
    BracketOrder, BrokerName, Direction, KronosSignal, OrderSide,
)


class StrategyEngine:
    """
    Stateless converter: (signal, account_equity) → BracketOrder | None
    """

    def __init__(self) -> None:
        self.sizing_mode = kats_cfg.position_sizing
        self.rr_ratio    = kats_cfg.default_rr_ratio
        self.risk_pct    = kats_cfg.max_position_risk_pct / 100.0

    def build_order(
        self,
        signal: KronosSignal,
        account_equity: float,
        broker: BrokerName = BrokerName.ALPACA,
        min_qty: float = 0.001,
        qty_precision: int = 3,
        trading_mode: TradingMode | None = None,
    ) -> BracketOrder | None:
        """Returns a fully populated BracketOrder or None if sizing fails."""
        if signal.direction == Direction.FLAT:
            return None

        entry = signal.entry_price
        atr   = signal.atr if signal.atr > 0 else entry * 0.001

        # ── Trading mode multipliers ──────────────────────────────────────────
        mode        = trading_mode or kats_cfg.trading_mode
        mode_params = TRADING_MODE_PARAMS.get(mode, {})
        size_scale  = mode_params.get("size_scale", 1.0)

        # kats_config.json overrides take precedence over mode defaults so
        # you can tune SL/TP without touching TRADING_MODE_PARAMS in code.
        sl_mult = (kats_cfg.sl_mult_override
                   if kats_cfg.sl_mult_override is not None
                   else mode_params.get("sl_mult", 1.0))
        tp_mult = (kats_cfg.tp_mult_override
                   if kats_cfg.tp_mult_override is not None
                   else mode_params.get("tp_mult", self.rr_ratio))

        # ── Stop loss / take profit ───────────────────────────────────────────
        if signal.direction == Direction.LONG:
            stop_loss   = entry - atr * sl_mult
            take_profit = entry + atr * tp_mult
            side        = OrderSide.BUY
        else:
            stop_loss   = entry + atr * sl_mult
            take_profit = entry - atr * tp_mult
            side        = OrderSide.SELL

        risk_per_unit = abs(entry - stop_loss)
        if risk_per_unit == 0:
            logger.warning(f"[strategy] zero risk_per_unit for {signal.symbol} — skip")
            return None

        # ── Position sizing ───────────────────────────────────────────────────
        dollar_risk = account_equity * self.risk_pct * size_scale

        if self.sizing_mode == "kelly":
            p       = signal.confidence
            b       = self.rr_ratio
            kelly_f = max(0.0, (p * b - (1.0 - p)) / b)
            kelly_f = min(kelly_f * 0.5, self.risk_pct * 2)
            dollar_risk = account_equity * kelly_f

        qty = dollar_risk / risk_per_unit

        # ── Cap notional to % of equity ───────────────────────────────────────
        max_notional = account_equity * kats_cfg.max_position_pct
        notional     = qty * entry
        if notional > max_notional:
            capped_qty = max_notional / entry
            logger.warning(
                f"[strategy] {signal.symbol} notional ${notional:,.0f} "
                f"exceeds cap ${max_notional:,.0f} "
                f"({kats_cfg.max_position_pct * 100:.0f}% equity) — "
                f"qty {qty:.4f} → {capped_qty:.4f}"
            )
            qty = capped_qty

        qty = round(max(qty, min_qty), qty_precision)

        order = BracketOrder(
            symbol=signal.symbol,
            side=side,
            quantity=qty,
            entry_price=entry,
            stop_loss=stop_loss,
            take_profit=take_profit,
            broker=broker,
            signal_id=f"{signal.symbol}_{signal.generated_at.isoformat()}",
        )

        logger.info(
            f"[strategy] {signal.symbol} {side.value.upper()} "
            f"qty={qty} entry={entry:.4f} sl={stop_loss:.4f} tp={take_profit:.4f} "
            f"notional=${qty * entry:,.0f} risk=${dollar_risk:.2f} "
            f"sizing={self.sizing_mode} mode={mode.value}"
        )
        return order

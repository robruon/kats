"""
kronos_trade/strategy/risk.py
Risk gatekeeper — the last check before any order reaches a broker.

Enforces:
  - Daily loss limit (% of starting equity)
  - Max drawdown halt
  - Max concurrent position count
  - Per-trade max risk
  - Kill switch (manual halt)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Optional

from loguru import logger

from kronos_trade.config import TRADING_MODE_PARAMS, kats_cfg
from kronos_trade.models import BracketOrder, Position


@dataclass
class RiskState:
    starting_equity:   float = 0.0
    peak_equity:       float = 0.0
    daily_start_equity: float = 0.0
    daily_date:        date = field(default_factory=date.today)
    daily_pnl:         float = 0.0
    daily_halted:      bool = False
    drawdown_halted:   bool = False
    kill_switch:       bool = False   # manual override
    rejected_count:    int = 0
    approved_count:    int = 0


class RiskGatekeeper:
    """
    All orders MUST pass through check_order() before submission.
    Call update_equity() after every account snapshot update.
    """

    def __init__(self) -> None:
        self.state = RiskState()
        self._cfg  = kats_cfg

    # ── State updates ─────────────────────────────────────────────────────────

    def initialize(self, equity: float) -> None:
        self.state.starting_equity    = equity
        self.state.peak_equity        = equity
        self.state.daily_start_equity = equity
        self.state.daily_date         = date.today()
        logger.info(f"[risk] initialized | equity=${equity:,.2f}")

    def update_equity(self, current_equity: float) -> None:
        today = date.today()

        # Roll daily equity on new calendar day
        if today != self.state.daily_date:
            self.state.daily_start_equity = current_equity
            self.state.daily_date         = today
            self.state.daily_pnl          = 0.0
            self.state.daily_halted       = False
            logger.info(f"[risk] new trading day — daily reset, equity=${current_equity:,.2f}")

        # Track daily P&L
        self.state.daily_pnl = current_equity - self.state.daily_start_equity

        # Update drawdown peak
        if current_equity > self.state.peak_equity:
            self.state.peak_equity = current_equity

        # Check halt conditions
        self._check_daily_halt()
        self._check_drawdown_halt(current_equity)

    def _check_daily_halt(self) -> None:
        if self.state.daily_start_equity == 0:
            return
        loss_pct = -self.state.daily_pnl / self.state.daily_start_equity * 100
        if loss_pct >= self._cfg.max_daily_loss_pct and not self.state.daily_halted:
            self.state.daily_halted = True
            logger.warning(
                f"[risk] ⚠ DAILY LOSS HALT triggered | "
                f"loss={loss_pct:.2f}% ≥ limit={self._cfg.max_daily_loss_pct}%"
            )

    def _check_drawdown_halt(self, equity: float) -> None:
        if self.state.peak_equity == 0:
            return
        dd_pct = (self.state.peak_equity - equity) / self.state.peak_equity * 100
        if dd_pct >= self._cfg.max_drawdown_pct and not self.state.drawdown_halted:
            self.state.drawdown_halted = True
            logger.warning(
                f"[risk] ⚠ DRAWDOWN HALT triggered | "
                f"dd={dd_pct:.2f}% ≥ limit={self._cfg.max_drawdown_pct}%"
            )

    # ── Order check ───────────────────────────────────────────────────────────

    def check_order(
        self,
        order: BracketOrder,
        open_positions: list[Position],
        account_equity: float,
        confidence: float = 0.0,
        trading_mode=None,
    ) -> tuple[bool, str]:
        """
        Returns (approved: bool, reason: str).
        reason is empty string on approval.
        """
        # Kill switch
        if self.state.kill_switch:
            return self._reject("kill switch active")

        # Daily halt
        if self.state.daily_halted:
            return self._reject(
                f"daily loss limit reached ({-self.state.daily_pnl:.2f})"
            )

        # Drawdown halt
        if self.state.drawdown_halted:
            return self._reject("max drawdown limit reached")

        # Trading mode confidence gate
        mode = trading_mode or self._cfg.trading_mode
        mode_params = TRADING_MODE_PARAMS.get(mode, {})
        min_conf = mode_params.get("confidence_min", self._cfg.min_signal_confidence)
        if confidence > 0 and confidence < min_conf:
            return self._reject(
                f"confidence {confidence:.2f} < mode minimum {min_conf:.2f} ({mode.value})"
            )

        # Concurrent position limit (count unique symbols, not entries)
        open_syms = {p.symbol for p in open_positions}
        n_open = len(open_syms)
        if n_open >= self._cfg.max_concurrent_positions and order.symbol not in open_syms:
            return self._reject(
                f"max concurrent positions ({self._cfg.max_concurrent_positions}) reached"
            )

        # Position scaling check
        if order.symbol in open_syms:
            if not self._cfg.allow_position_scaling:
                return self._reject(f"position already open for {order.symbol}")
            # Count existing entries for this symbol
            entry_count = sum(
                max(len(p.entries), 1)
                for p in open_positions
                if p.symbol == order.symbol
            )
            if entry_count >= self._cfg.max_entries_per_symbol:
                return self._reject(
                    f"max entries ({self._cfg.max_entries_per_symbol}) reached for {order.symbol}"
                )

        # Per-trade risk check
        trade_risk = abs(order.entry_price - order.stop_loss) * order.quantity
        max_trade_risk = account_equity * self._cfg.max_position_risk_pct / 100
        if trade_risk > max_trade_risk * 1.5:   # allow 50% buffer for slippage
            return self._reject(
                f"trade risk ${trade_risk:.2f} > max ${max_trade_risk:.2f}"
            )

        self.state.approved_count += 1
        logger.info(f"[risk] ✓ approved | {order.symbol} {order.side.value}")
        return True, ""

    def _reject(self, reason: str) -> tuple[bool, str]:
        self.state.rejected_count += 1
        logger.warning(f"[risk] ✗ rejected | {reason}")
        return False, reason

    # ── Manual controls ───────────────────────────────────────────────────────

    def engage_kill_switch(self) -> None:
        self.state.kill_switch = True
        logger.warning("[risk] 🔴 KILL SWITCH ENGAGED — no new orders will be sent")

    def disengage_kill_switch(self) -> None:
        self.state.kill_switch = False
        logger.info("[risk] 🟢 kill switch disengaged")

    def reset_daily_halt(self) -> None:
        """Manual override — only use if you know what you're doing."""
        self.state.daily_halted = False
        logger.warning("[risk] daily halt manually reset")

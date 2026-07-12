"""
kronos_trade/utils/schedule.py

Trading schedule parser and gate.

Format:  one or more comma-separated DAYS:STARTEND tokens.
  DAYS     — any non-empty subset of 1234567 (1=Mon … 7=Sun)
  START    — 4-digit HHMM (UTC) when the window opens
  END      — 4-digit HHMM (UTC) when the window closes

Special case: START == END == "0000" → always active (no restriction).
Overnight windows are supported: when START > END the window wraps midnight.

Single window examples:
  "1234567:00000000"  — all days, full day (no restriction — same as no schedule)
  "12345:09001700"    — weekdays 09:00–17:00 UTC
  "135:08301230"      — Mon/Wed/Fri 08:30–12:30 UTC
  "7:22002359"        — Sunday 22:00–23:59 UTC

Multi-window example (full forex week):
  "12345:08002200,7:22002359"
      Mon–Fri 08:00–22:00 UTC  (London open through NY close)
    + Sunday 22:00–23:59 UTC   (Sunday evening open)
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone

_TOKEN = re.compile(r"^([1-7]+):(\d{4})(\d{4})$")

_DAY_NAMES = {1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu",
              5: "Fri", 6: "Sat", 7: "Sun"}


class _Window:
    """A single DAYS:STARTEND trading window."""

    def __init__(self, spec: str) -> None:
        m = _TOKEN.match(spec.strip())
        if not m:
            raise ValueError(
                f"Invalid window spec {spec!r}. "
                "Expected DAYS:STARTEND  e.g. '12345:09001700'"
            )
        self.spec       = spec.strip()
        self.days       = {int(d) for d in m.group(1)}
        self.start_hhmm = int(m.group(2))
        self.end_hhmm   = int(m.group(3))

    @property
    def always_active(self) -> bool:
        return self.start_hhmm == 0 and self.end_hhmm == 0

    def is_active(self, dt: datetime) -> bool:
        if self.always_active:
            return True
        if dt.isoweekday() not in self.days:
            return False
        hhmm = dt.hour * 100 + dt.minute
        if self.start_hhmm < self.end_hhmm:
            # Normal window e.g. 0900–1700
            return self.start_hhmm <= hhmm < self.end_hhmm
        else:
            # Overnight window e.g. 2200–0600 (wraps midnight)
            return hhmm >= self.start_hhmm or hhmm < self.end_hhmm

    def __str__(self) -> str:
        if self.always_active:
            return "always active"
        days_str = "/".join(_DAY_NAMES[d] for d in sorted(self.days))
        sh, sm   = divmod(self.start_hhmm, 100)
        eh, em   = divmod(self.end_hhmm,   100)
        return f"{days_str} {sh:02d}:{sm:02d}–{eh:02d}:{em:02d} UTC"


class TradingSchedule:
    """
    One or more comma-separated trading windows.

    Public interface (used by router.py and run_system.py):
        schedule.is_active()   → bool
        schedule.next_open()   → datetime
        str(schedule)          → human-readable description
    """

    def __init__(self, spec: str) -> None:
        self.spec     = spec.strip()
        self._windows = [_Window(t.strip()) for t in spec.split(",") if t.strip()]
        if not self._windows:
            raise ValueError(f"Empty schedule spec {spec!r}")

    @property
    def always_active(self) -> bool:
        """True if any window is the always-active sentinel (0000–0000)."""
        return any(w.always_active for w in self._windows)

    def is_active(self, dt: datetime | None = None) -> bool:
        """Return True if *dt* (default: now UTC) falls inside any window."""
        now = dt or datetime.now(tz=timezone.utc)
        return any(w.is_active(now) for w in self._windows)

    def next_open(self, dt: datetime | None = None) -> datetime:
        """
        Return the next UTC datetime when any window opens.
        Walks forward in 1-minute steps up to 8 days.
        """
        if self.always_active:
            return dt or datetime.now(tz=timezone.utc)
        now       = dt or datetime.now(tz=timezone.utc)
        candidate = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
        for _ in range(8 * 24 * 60):
            if self.is_active(candidate):
                return candidate
            candidate += timedelta(minutes=1)
        return candidate  # fallback

    def __str__(self) -> str:
        if self.always_active:
            return "always active"
        return " | ".join(str(w) for w in self._windows)

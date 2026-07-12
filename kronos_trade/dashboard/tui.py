"""
KronosTrade control panel — Textual TUI.

Layout (using dock + vertical/horizontal layout):
  ┌─ StatusBar (docked top) ──────────────────────────────┐
  │─ PriceTicker (docked top) ────────────────────────────│
  │                                                       │
  │  ┌─ AccountPanel ─────┐  ┌─ RiskPanel ──────────────┐ │
  │  │                    │  │                          │ │
  │  └────────────────────┘  └──────────────────────────┘ │
  │  ┌─ SignalPanel ────────────────────────────────────┐ │
  │  └──────────────────────────────────────────────────┘ │
  │  ┌─ Positions ────────┐  ┌─ Event Log ──────────────┐ │
  │  │                    │  │                          │ │
  │  └────────────────────┘  └──────────────────────────┘ │
  │                                                       │
  │─ CloseBar (docked bottom, visible when positions) ────│
  │─ KillRow  (docked bottom) ────────────────────────────│
  └─ Footer   (docked bottom) ────────────────────────────┘
"""
from __future__ import annotations

import asyncio
import json
import time as _time
from datetime import datetime, timezone

import httpx
import websockets
from rich.text import Text
from textual import on, work
from textual.app import App, ComposeResult
from textual.message import Message
from textual.binding import Binding
from textual.containers import Container, Horizontal, Vertical
from textual.reactive import reactive
from textual.screen import ModalScreen
from textual.widget import Widget
from textual.widgets import (
    Checkbox, DataTable, Header,
    Input, Label, RichLog, Select, Static,
)

from kronos_trade.config import kats_cfg, settings

API_BASE = settings.api_url
WS_URL   = settings.api_ws_url


# ── Docked top panels ─────────────────────────────────────────────────────────

class StatusBar(Static):
    status:    reactive[str]  = reactive("● CONNECTING")
    kronos:    reactive[str]  = reactive("Kronos: loading")
    uptime:    reactive[str]  = reactive("00:00:00")
    broker:    reactive[str]  = reactive("")
    mode:      reactive[str]  = reactive("day")
    timeframe: reactive[str]  = reactive("1h")
    paused:    reactive[bool] = reactive(False)
    standby:   reactive[str]  = reactive("")   # non-empty → in trading schedule standby

    def watch_status(self,    _: str)  -> None: self.refresh()
    def watch_kronos(self,    _: str)  -> None: self.refresh()
    def watch_uptime(self,    _: str)  -> None: self.refresh()
    def watch_broker(self,    _: str)  -> None: self.refresh()
    def watch_mode(self,      _: str)  -> None: self.refresh()
    def watch_timeframe(self, _: str)  -> None: self.refresh()
    def watch_paused(self,    _: bool) -> None: self.refresh()
    def watch_standby(self,   _: str)  -> None: self.refresh()

    def render(self) -> Text:
        t = Text()
        t.append("  KronosTrade  ", style="bold white on dark_blue")
        if self.standby:
            t.append("  ⏸ STANDBY  ", style="bold white on dark_red")
            t.append(f"  Next: {self.standby}  ", style="bold yellow")
        else:
            t.append(f"  {self.status}  ", style="bold green")
            if self.paused:
                t.append("  ⏸ KRONOS PAUSED  ", style="bold yellow on dark_red")
            else:
                t.append(f"  {self.kronos}  ", style="bold magenta")
        if self.broker:
            t.append(f"  broker={self.broker}  ", style="bold cyan")
        mode_style = {"scalping": "bold yellow", "day": "bold blue", "swing": "bold green"}.get(self.mode, "dim")
        t.append(f"  mode={self.mode}  ", style=mode_style)
        t.append(f"  tf={self.timeframe}  ", style="bold white")
        t.append(f"  {self.uptime}", style="dim")
        return t


class PriceTicker(Static):
    DEFAULT_CSS = """
    PriceTicker {
        height: 1;
        width: 100%;
        padding: 0 0;
        background: $boost;
    }
    """
    _prices:      dict[str, float] = {}
    _prev_prices: dict[str, float] = {}
    _offset:      int  = 0
    _ticker_text: Text = Text()   # full one-cycle Rich Text (built on price update)

    # ── Scroll timer ─────────────────────────────────────────────────────────────
    def on_mount(self) -> None:
        # One character per tick at 80 ms ≈ comfortable reading speed
        self.set_interval(0.09, self._tick)

    def _tick(self) -> None:
        length = len(self._ticker_text.plain)
        if length:
            self._offset = (self._offset + 1) % length
            self.refresh()

    # ── Price update ──────────────────────────────────────────────────────────────
    def update_prices(self, prices: dict, prev: dict) -> None:
        self._prices      = prices
        self._prev_prices = prev
        if not prices:
            self._ticker_text = Text()
            self.refresh()
            return

        t = Text()
        for sym in sorted(prices):
            p     = prices[sym]
            old   = prev.get(sym, p)
            diff  = p - old
            color = "green" if diff >= 0 else "red"
            arrow = "▲" if diff >= 0 else "▼"
            t.append(f"  {sym} ", style="bold white")
            t.append(f"{p:,.4f} {arrow}", style=color)
            t.append("  ·  ", style="dim")

        self._ticker_text = t
        # Keep offset in bounds after a symbol count change
        if len(t.plain):
            self._offset %= len(t.plain)
        self.refresh()

    # ── Render ────────────────────────────────────────────────────────────────────
    def render(self) -> Text:
        if not self._ticker_text.plain:
            return Text.from_markup("  [dim]awaiting prices…[/dim]")
        # Double the text so a seamless wrap is always available at any offset.
        # Rich's Text.__add__ and __getitem__ preserve all colour spans correctly.
        doubled = self._ticker_text + self._ticker_text
        return doubled[self._offset:]


# ── Side panels ───────────────────────────────────────────────────────────────

class AccountPanel(Static):
    DEFAULT_CSS = """
    AccountPanel {
        width: 1fr;
        border: solid $primary;
        padding: 1 2;
        height: auto;
    }
    """

    def compose(self) -> ComposeResult:
        yield Label("Account", classes="panel-title")
        yield Static("Equity:     —", id="acct-equity")
        yield Static("Cash:       —", id="acct-cash")
        yield Static("Daily P&L:  —", id="acct-daily-pnl")
        yield Static("Unreal P&L: —", id="acct-unreal-pnl")

    def update(self, data: dict) -> None:
        eq = data.get("equity",        0) or 0
        ca = data.get("cash",          0) or 0
        dp = data.get("daily_pnl",     0) or 0
        up = data.get("unrealized_pnl",0) or 0
        if eq == 0 and ca == 0:
            return
        pc = "green" if dp >= 0 else "red"
        self.query_one("#acct-equity").update(    f"Equity:     ${eq:>12,.2f}")
        self.query_one("#acct-cash").update(      f"Cash:       ${ca:>12,.2f}")
        self.query_one("#acct-daily-pnl").update(
            Text.from_markup(f"Daily P&L:  [{pc}]${dp:>+12,.2f}[/{pc}]")
        )
        uc = "green" if up >= 0 else "red"
        self.query_one("#acct-unreal-pnl").update(
            Text.from_markup(f"Unreal P&L: [{uc}]${up:>+12,.2f}[/{uc}]")
        )


class RiskPanel(Static):
    DEFAULT_CSS = """
    RiskPanel {
        width: 1fr;
        border: solid $primary;
        padding: 1 2;
        height: auto;
    }
    """

    def compose(self) -> ComposeResult:
        yield Label("Risk", classes="panel-title")
        yield Static("Daily halt:  [green]OK[/green]",      id="r-daily")
        yield Static("DD halt:     [green]OK[/green]",      id="r-dd")
        yield Static("Positions:   0",                      id="r-pos")
        yield Static("Mode:        [blue]day[/blue]",       id="r-mode")
        yield Static("Timeframe:   [white]1h[/white]",       id="r-tf")
        yield Static("Kronos:      [green]▶ running[/green]", id="r-kronos")
        yield Checkbox("Kill switch", id="kill-switch-toggle", value=False)

    def update(self, state: dict) -> None:
        def _v(flag: bool, halt_txt: str, ok_txt: str, halt_style: str = "red bold") -> Text:
            style = halt_style if flag else "green"
            return Text.from_markup(f"[{style}]{halt_txt if flag else ok_txt}[/{style}]")

        daily = state.get("daily_loss_halted", False)
        dd    = state.get("drawdown_halted",   False)
        npos  = len(state.get("positions",     []))

        self.query_one("#r-daily").update(
            Text.assemble("Daily halt:  ", _v(daily, "HALTED", "OK"))
        )
        self.query_one("#r-dd").update(
            Text.assemble("DD halt:     ", _v(dd, "HALTED", "OK"))
        )
        self.query_one("#r-pos").update(
            f"Positions:   {npos} / {kats_cfg.max_concurrent_positions}"
        )
        paused = state.get("kronos_paused", False)
        if paused:
            self.query_one("#r-kronos").update(
                Text.from_markup("Kronos:      [yellow bold]⏸ paused[/yellow bold]")
            )
        else:
            self.query_one("#r-kronos").update(
                Text.from_markup("Kronos:      [green]▶ running[/green]")
            )


# ── Signal panel ───────────────────────────────────────────────────────────────

class SignalPanel(Static):

    DEFAULT_CSS = """
    SignalPanel {
        border: solid $accent;
        padding: 1 2;
        height: auto;
    }
    """

    def compose(self) -> ComposeResult:
        yield Label("Kronos Signal", classes="panel-title")
        yield Static("[dim]No signal yet[/dim]", id="sig-main")
        yield Static("", id="sig-rr")
        yield Static("", id="sig-stats")

    def update(self, signal: dict) -> None:
        sym   = signal.get("symbol",    "—")
        dirn  = (signal.get("direction") or "flat").upper()
        conf  = (signal.get("confidence") or 0) * 100
        ts    = (signal.get("generated_at") or "")[:19].replace("T", " ")
        entry = signal.get("entry_price",         0) or 0
        vol   = signal.get("volatility_forecast", 0) or 0
        atr   = signal.get("atr",                 0) or 0

        ds = {"LONG": "green bold", "SHORT": "red bold"}.get(dirn, "dim")
        self.query_one("#sig-main").update(
            Text.from_markup(
                f"[bold]{sym}[/bold]  [{ds}]{dirn}[/{ds}]  "
                f"conf=[yellow]{conf:.1f}%[/yellow]  entry={entry:.4f}"
            )
        )

        # ── R:R geometry bar ─────────────────────────────────────────────────
        mean_fc = signal.get("forecast_mean",  [])
        lower   = signal.get("forecast_lower", [])
        upper   = signal.get("forecast_upper", [])

        from kronos_trade.config import TRADING_MODE_PARAMS, kats_cfg as _kc
        mp     = TRADING_MODE_PARAMS.get(_kc.trading_mode, {})
        sl_m   = mp.get("sl_mult",  1.0)
        tp_m   = mp.get("tp_mult",  _kc.default_rr_ratio)
        atr_v  = atr if atr > 0 else (entry * 0.001)

        if dirn == "LONG":
            sl = entry - atr_v * sl_m
            tp = entry + atr_v * tp_m
        elif dirn == "SHORT":
            sl = entry + atr_v * sl_m
            tp = entry - atr_v * tp_m
        else:
            sl = tp = entry

        fc_end  = mean_fc[-1] if mean_fc else entry
        sl_pct  = (sl - entry)  / entry * 100 if entry else 0
        tp_pct  = (tp - entry)  / entry * 100 if entry else 0
        fc_pct  = (fc_end - entry) / entry * 100 if entry else 0

        # Build a fixed-width bar: SL ←——[entry]══════[fc_end]——→ TP
        BAR = 28
        if dirn == "LONG":
            sl_pos  = 0
            tp_pos  = BAR - 1
            ent_pos = int((entry - sl) / max(tp - sl, 1e-9) * BAR)
            fc_pos  = int((fc_end - sl) / max(tp - sl, 1e-9) * BAR)
        elif dirn == "SHORT":
            sl_pos  = BAR - 1
            tp_pos  = 0
            ent_pos = int((sl - entry) / max(sl - tp, 1e-9) * BAR)
            fc_pos  = int((sl - fc_end) / max(sl - tp, 1e-9) * BAR)
        else:
            ent_pos = fc_pos = BAR // 2

        ent_pos = max(1, min(BAR - 2, ent_pos))
        fc_pos  = max(0, min(BAR - 1, fc_pos))

        bar_chars = [" "] * BAR
        # Fill between entry and fc_end
        lo, hi = sorted([ent_pos, fc_pos])
        for i in range(lo, hi + 1):
            bar_chars[i] = "═"
        bar_chars[ent_pos] = "┼"
        bar_chars[fc_pos]  = "◆" if fc_pos != ent_pos else "┼"

        fc_color = "green" if fc_pct >= 0 else "red"
        sl_label = f"SL {sl_pct:+.2f}%"
        tp_label = f"TP {tp_pct:+.2f}%"
        rr_bar   = "".join(bar_chars)

        t = Text()
        t.append(f"{sl_label} ", style="red")
        t.append(rr_bar, style=fc_color)
        t.append(f" {tp_label}", style="green")
        self.query_one("#sig-rr").update(t)

        # ── Stats row ─────────────────────────────────────────────────────────
        n_bars   = len(mean_fc)
        bullish  = sum(1 for v in mean_fc if v > entry) if mean_fc else 0
        peak     = max(mean_fc) if mean_fc else entry
        peak_i   = mean_fc.index(peak) + 1 if mean_fc else 0
        unc_end  = (upper[-1] - lower[-1]) if upper and lower else 0

        self.query_one("#sig-stats").update(
            Text.from_markup(
                f"[dim]fc→TP[/dim] [green]{fc_pct:+.2f}%[/green]  "
                f"[dim]fc→SL[/dim] [red]{sl_pct:.2f}%[/red]  "
                f"[dim]bull[/dim] {bullish}/{n_bars}  "
                f"[dim]peak[/dim] {peak:.4f}[dim]@#{peak_i}[/dim]  "
                f"[dim]unc[/dim] ±{unc_end:.4f}  "
                f"[dim]vol[/dim] {vol:.4f}"
            )
        )


# ── Positions table ────────────────────────────────────────────────────────────

class PositionsTable(DataTable):

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._expanded:  set[str]   = set()
        self._positions: list[dict] = []

    def on_mount(self) -> None:
        self.cursor_type = "row"
        self.add_columns("Symbol", "Side", "Qty", "Entry", "Current", "SL", "TP", "P&L")

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        """Enter on a multi-entry row toggles expansion."""
        sym = self._key_to_symbol(str(event.row_key.value))
        if not sym:
            return
        p = next((p for p in self._positions if p.get("symbol") == sym), None)
        if p and len(p.get("entries", [])) > 1:
            if sym in self._expanded:
                self._expanded.discard(sym)
            else:
                self._expanded.add(sym)
            self._render()

    def _key_to_symbol(self, key: str) -> str | None:
        """Row keys are the plain symbol string."""
        return key if key and not key.startswith("  └") else None

    def refresh_positions(self, positions: list[dict]) -> None:
        self._positions = positions
        self._render()

    # ── Entry row helpers ─────────────────────────────────────────────────────

    @staticmethod
    def _fmt_exit_cell(price: float, order_id: str, is_sl: bool) -> Text:
        """
        Format a SL or TP cell for an entry row.
          SL → red   `▼ 1234.5000 [abc123]`
          TP → green `▲ 1234.5000 [abc123]`
        Falls back to a plain dash when price is zero.
        """
        if not price:
            return Text("—")
        arrow  = "▼" if is_sl else "▲"
        colour = "red" if is_sl else "green"
        t = Text(f"{arrow} {price:.4f}", style=colour)
        if order_id:
            slug = order_id[-6:] if len(order_id) > 6 else order_id
            t.append(f" [{slug}]", style=f"dim {colour}")
        return t

    @staticmethod
    def _make_pos_bar(sl: float, tp: float, current: float, width: int = 14) -> Text:
        """
        Render a mini horizontal bar showing where `current` sits between SL and TP.
        Example:  ──────●────   (● slides left→right as price moves SL→TP)
        Falls back to a plain price string when SL/TP are missing or equal.
        """
        if not sl or not tp or sl == tp:
            return Text(f"{current:.4f}", style="dim")
        lo, hi = (sl, tp) if sl < tp else (tp, sl)
        pos    = max(0.0, min(1.0, (current - lo) / (hi - lo)))
        bar_w  = width - 1          # 1 char reserved for the bullet
        idx    = round(pos * bar_w)
        left   = "─" * idx
        right  = "─" * (bar_w - idx)
        is_long = tp > sl
        # green when price is on the profitable side, red when near stop
        hue = "green" if (is_long and pos > 0.5) or (not is_long and pos < 0.5) else "red"
        t = Text()
        t.append(left,  style="dim")
        t.append("●",   style=f"bold {hue}")
        t.append(right, style="dim")
        return t

    # ── Main render ───────────────────────────────────────────────────────────

    def _render(self) -> None:
        selected = self.selected_symbol()
        self.clear()
        next_row = None
        for p in self._positions:
            pnl     = p.get("unrealized_pnl", 0) or 0
            dirn    = (p.get("direction") or "").upper()
            sl      = p.get("stop_loss",    0) or 0
            tp      = p.get("take_profit",  0) or 0
            cur     = p.get("current_price", p.get("entry_price", 0)) or 0
            dc      = "green" if dirn == "LONG" else "red"
            pc      = "green" if pnl  >= 0 else "red"
            symbol  = p.get("symbol", "—")
            entries = p.get("entries", [])
            n_ent   = len(entries)
            expanded = symbol in self._expanded and n_ent > 1
            chevron  = "▼" if expanded else ("▶" if n_ent > 1 else " ")
            sym_label = f"{chevron} {symbol} ×{n_ent}" if n_ent > 1 else f"  {symbol}"
            if symbol == selected:
                next_row = self.row_count
            # Multi-entry: side/SL/TP live on child rows — always blank on parent
            multi  = n_ent > 1
            broker = p.get("broker", "")
            qty    = p.get("quantity", 0) or 0
            # OANDA uses units (e.g. 1000).  Show as lots (÷100,000) so the
            # number is comparable to what prop-firm dashboards display.
            qty_str = (f"{qty / 100_000:.2f}L" if broker == "oanda"
                       else f"{qty:.4f}")
            self.add_row(
                sym_label,
                Text(dirn, style=dc) if not multi else Text("—", style="dim"),
                qty_str,
                "—" if multi else f"{p.get('entry_price', 0):.5f}",
                f"{cur:.5f}",
                "—" if multi else (f"{sl:.5f}" if sl else "—"),
                "—" if multi else (f"{tp:.5f}" if tp else "—"),
                Text(f"${pnl:+.2f}", style=pc),
                key=symbol,
            )
            if expanded:
                for idx, e in enumerate(entries, start=1):
                    e_sl    = e.get("stop_loss",   0) or 0
                    e_tp    = e.get("take_profit",  0) or 0
                    e_sl_id = e.get("sl_order_id", "") or ""
                    e_tp_id = e.get("tp_order_id", "") or ""
                    e_qty   = e.get("quantity", 0) or 0
                    e_qty_str = (f"{e_qty / 100_000:.2f}L" if broker == "oanda"
                                 else f"{e_qty:.4f}")
                    self.add_row(
                        f"  └─ #{idx}",
                        Text((e.get("direction") or dirn).upper()[:1], style=dc),
                        e_qty_str,
                        f"{e.get('entry_price', 0):.5f}",
                        self._make_pos_bar(e_sl, e_tp, cur),
                        self._fmt_exit_cell(e_sl, e_sl_id, is_sl=True),
                        self._fmt_exit_cell(e_tp, e_tp_id, is_sl=False),
                        "",
                    )
        if self.row_count:
            self.move_cursor(row=next_row or 0, animate=False, scroll=False)

    def selected_symbol(self) -> str | None:
        try:
            if self.row_count == 0:
                return None
            row = self.get_row_at(self.cursor_row)
            val = str(row[0]).strip().lstrip("▶▼ ")
            # Strip "×N" suffix
            return val.split(" ×")[0] if val else None
        except Exception:
            return None


# ── Close modal ───────────────────────────────────────────────────────────────

class CloseModal(ModalScreen):
    """
    Modal overlay for close actions.
    Captures all key events regardless of widget focus,
    so digit keys 1-4 always work.
    """
    DEFAULT_CSS = """
    CloseModal {
        align: center middle;
    }
    CloseModal > #close-modal-box {
        width: 48;
        height: auto;
        border: thick $primary;
        background: $surface;
        padding: 1 2;
    }
    CloseModal > #close-modal-box > Label {
        width: 100%;
        text-align: center;
        margin-bottom: 1;
    }
    CloseModal > #close-modal-box > Static {
        width: 100%;
    }
    """

    BINDINGS = [
        Binding("1", "close_1",  show=False),
        Binding("2", "close_2",  show=False),
        Binding("3", "close_3",  show=False),
        Binding("4", "close_4",  show=False),
        Binding("escape", "dismiss", show=False),
        Binding("c",      "dismiss", show=False),
    ]

    def __init__(self, positions: list[dict], http: httpx.AsyncClient) -> None:
        super().__init__()
        self._positions = positions
        self._http = http

    def compose(self) -> ComposeResult:
        with Container(id="close-modal-box"):
            yield Label("[bold]Close positions[/bold]")
            yield Static(" [reverse] 1 [/reverse]  Close selected")
            yield Static(" [reverse] 2 [/reverse]  Close all winning ▲")
            yield Static(" [reverse] 3 [/reverse]  Close all losing ▼")
            yield Static(" [reverse] 4 [/reverse]  Close ALL positions")
            yield Static("[dim]  ESC / C — cancel[/dim]")

    async def action_close_1(self) -> None:
        self.dismiss()
        # Get selected symbol from the main screen's positions table
        try:
            tbl = self.app.query_one("#positions-table", PositionsTable)
            sym = tbl.selected_symbol()
            if not sym:
                self.app._log("[dim]No position selected — use ↑↓[/dim]")
                return
            await self._http.post("/close-position", json={"symbol": sym})
            self.app._log(f"[yellow]Close submitted: {sym}[/yellow]")
        except Exception as exc:
            self.app._log(f"[red]Close failed: {exc}[/red]")

    async def action_close_2(self) -> None:
        self.dismiss()
        try:
            r      = await self._http.post("/close-winning")
            closed = r.json().get("closed", [])
            self.app._log(
                f"[green]Closed winning: {', '.join(closed)}[/green]"
                if closed else "[dim]No winning positions[/dim]"
            )
        except Exception as exc:
            self.app._log(f"[red]Close winning failed: {exc}[/red]")

    async def action_close_3(self) -> None:
        self.dismiss()
        try:
            r      = await self._http.post("/close-losing")
            closed = r.json().get("closed", [])
            self.app._log(
                f"[red]Closed losing: {', '.join(closed)}[/red]"
                if closed else "[dim]No losing positions[/dim]"
            )
        except Exception as exc:
            self.app._log(f"[red]Close losing failed: {exc}[/red]")

    async def action_close_4(self) -> None:
        self.dismiss()
        try:
            await self._http.post("/close-all")
            self.app._log("[yellow]Close all submitted[/yellow]")
        except Exception as exc:
            self.app._log(f"[red]Close all failed: {exc}[/red]")


# ── Instruments panel (docked right) ─────────────────────────────────────────

class InstrumentsPanel(Widget):
    """
    Docked right-side panel: scrollable, filterable broker symbol list.
    Toggle with `i`.  Enter = toggle selection.  Ctrl+S = save.  Esc = close.
    """

    # priority=True fires BEFORE the focused descendant (Input or DataTable)
    # processes the key, so ctrl+s and escape always work regardless of focus.
    BINDINGS = [
        Binding("ctrl+s", "save_instruments", "Save",  show=False, priority=True),
        Binding("escape",  "close_panel",     "Close", show=False, priority=True),
    ]

    class SaveSelected(Message):
        def __init__(self, instruments: list[str]) -> None:
            super().__init__()
            self.instruments = instruments

    class ClosePanel(Message):
        pass

    DEFAULT_CSS = """
    InstrumentsPanel {
        dock: right;
        width: 32;
        border: solid $primary;
        background: $surface;
        padding: 0;
        display: none;
    }
    InstrumentsPanel.active {
        display: block;
    }
    #ip-title {
        width: 100%;
        text-align: center;
        text-style: bold;
        background: $boost;
        padding: 0 0;
        height: 1;
    }
    #ip-status {
        width: 100%;
        text-align: center;
        height: 1;
    }
    #ip-filter {
        width: 100%;
        height: 3;
        margin: 0;
    }
    #ip-table {
        height: 1fr;
        border: none;
    }
    #ip-hint {
        width: 100%;
        text-align: center;
        height: 1;
    }
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._all_crypto: list[str] = []
        self._all_forex:  list[str] = []   # forex, metals, CFDs, indices, unknowns
        self._all_equity: list[str] = []   # equities — search-only (large lists)
        self._selected:   set[str]  = set(kats_cfg.instrument_list)
        self._filter:     str       = ""
        self._visible:    list[str] = []   # parallel list matching DataTable rows
        self._loaded:     bool      = False

    def compose(self) -> ComposeResult:
        yield Label("Instruments", id="ip-title")
        yield Static("[dim]loading…[/dim]", id="ip-status")
        yield Input(placeholder="Search symbols…", id="ip-filter")
        yield DataTable(id="ip-table", show_header=False, cursor_type="row")
        yield Static("[dim]Enter=toggle  Ctrl+S=save  Esc=close[/dim]", id="ip-hint")

    def on_mount(self) -> None:
        tbl = self.query_one("#ip-table", DataTable)
        tbl.add_column("sym", width=28)
        self._rebuild_table()

    @work(thread=False)
    async def load_symbols(self, http: httpx.AsyncClient) -> None:
        try:
            resp = await http.get("/instruments/available")
            data = resp.json()
            crypto, forex, equity = [], [], []
            for item in data.get("symbols", []):
                sym = item["symbol"]
                ac  = str(item.get("asset_class", "")).lower()
                if "crypto" in ac:
                    crypto.append(sym)
                elif ac == "equity":
                    # Equities can number in the thousands (Alpaca) — don't
                    # show them all at once; require a search term instead.
                    equity.append(sym)
                else:
                    # forex, metal, index, cfd, futures, unknown → show
                    # immediately so OANDA / NinjaTrader users see their
                    # tradeable universe without having to know what to type.
                    forex.append(sym)
            self._all_crypto = sorted(crypto)
            self._all_forex  = sorted(forex)
            self._all_equity = sorted(equity)
        except Exception:
            self._all_crypto = list(kats_cfg.instrument_list)
            self._all_forex  = []
            self._all_equity = []
        self._loaded = True
        self._rebuild_table()

    def _rebuild_table(self) -> None:
        filt = self._filter.strip().upper()
        tbl  = self.query_one("#ip-table", DataTable)
        tbl.clear()
        self._visible = []

        selected_sorted = sorted(self._selected)

        # Crypto: always visible — small list, all relevant.
        crypto_unsel = [s for s in self._all_crypto if s not in self._selected]

        # Forex / metals / CFDs / unknowns: always visible, filter when searching.
        if filt:
            forex_unsel = [
                s for s in self._all_forex
                if s not in self._selected and filt in s
            ]
        else:
            forex_unsel = [s for s in self._all_forex if s not in self._selected]

        # Equities: search-only — Alpaca equity lists can be 5000+ symbols.
        if filt:
            equity_unsel = [
                s for s in self._all_equity
                if s not in self._selected and filt in s
            ][:100]
        else:
            equity_unsel = []

        items = selected_sorted + crypto_unsel + forex_unsel + equity_unsel
        self._visible = items

        for sym in items:
            if sym in self._selected:
                tbl.add_row(Text(f"● {sym}", style="bold green"))
            else:
                tbl.add_row(Text(f"  {sym}", style="dim"))

        total   = len(self._all_crypto) + len(self._all_forex) + len(self._all_equity)
        n_shown = len(items)
        hint    = "" if filt or not self._all_equity else f"  [dim]+{len(self._all_equity)} eq[/dim]"
        if self._loaded:
            self.query_one("#ip-status", Static).update(
                f"[bold]{len(self._selected)}[/bold] sel  "
                f"[dim]{n_shown}/{total}[/dim]{hint}"
            )
        else:
            self.query_one("#ip-status", Static).update("[dim]loading…[/dim]")

    def _toggle_cursor(self) -> None:
        tbl = self.query_one("#ip-table", DataTable)
        row = tbl.cursor_row
        if 0 <= row < len(self._visible):
            sym = self._visible[row]
            if sym in self._selected:
                self._selected.discard(sym)
            else:
                self._selected.add(sym)
            self._rebuild_table()
            # Keep cursor near the same position after rebuild
            tbl.move_cursor(row=min(row, tbl.row_count - 1), animate=False)

    @on(DataTable.RowSelected, "#ip-table")
    def _on_row_selected(self, event: DataTable.RowSelected) -> None:
        """Enter key on the DataTable toggles the highlighted symbol."""
        self._toggle_cursor()
        event.stop()

    @on(Input.Changed, "#ip-filter")
    def _on_filter_changed(self, event: Input.Changed) -> None:
        self._filter = event.value
        self._rebuild_table()

    def action_save_instruments(self) -> None:
        self.post_message(self.SaveSelected(sorted(self._selected)))

    def action_close_panel(self) -> None:
        self.post_message(self.ClosePanel())


# ── Kronos live settings panel ────────────────────────────────────────────────

class KronosPanel(Widget):
    """
    Docked right-side panel: live Kronos model param editor.
    Toggle with `x`.  Ctrl+S = apply & restart engine.  Esc = close.
    """

    BINDINGS = [
        Binding("ctrl+s", "apply_settings", "Apply", show=False, priority=True),
        Binding("escape",  "close_panel",   "Close", show=False, priority=True),
    ]

    class ApplySettings(Message):
        def __init__(self, cfg: dict) -> None:
            super().__init__()
            self.cfg = cfg

    class ClosePanel(Message):
        pass

    DEFAULT_CSS = """
    KronosPanel {
        dock: right;
        width: 36;
        border: solid $accent;
        background: $surface;
        padding: 0;
        display: none;
    }
    KronosPanel.active {
        display: block;
    }
    #kp-title {
        width: 100%;
        text-align: center;
        text-style: bold;
        background: $boost;
        height: 1;
    }
    #kp-status {
        width: 100%;
        text-align: center;
        height: 1;
    }
    #kp-hint {
        width: 100%;
        text-align: center;
        height: 1;
    }
    .kp-row {
        height: 3;
    }
    .kp-label {
        width: 14;
        padding: 1 1;
        text-style: bold;
    }
    .kp-select {
        width: 1fr;
    }
    .kp-input {
        width: 1fr;
    }
    """

    _MODEL_SIZES = ["mini", "small", "base"]
    _DEVICES     = ["cpu", "mps", "cuda"]

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def compose(self) -> ComposeResult:
        yield Label("⚙ Kronos Settings", id="kp-title")
        yield Static("", id="kp-status")

        with Horizontal(classes="kp-row"):
            yield Label("Model size", classes="kp-label")
            yield Select(
                [(s, s) for s in self._MODEL_SIZES],
                value=kats_cfg.kronos_model_size,
                id="kp-model-size",
                classes="kp-select",
                allow_blank=False,
            )

        with Horizontal(classes="kp-row"):
            yield Label("Device", classes="kp-label")
            yield Select(
                [(d, d) for d in self._DEVICES],
                value=kats_cfg.kronos_device,
                id="kp-device",
                classes="kp-select",
                allow_blank=False,
            )

        with Horizontal(classes="kp-row"):
            yield Label("Max context", classes="kp-label")
            yield Input(str(kats_cfg.kronos_max_context), id="kp-max-context",
                        classes="kp-input", placeholder="512")

        with Horizontal(classes="kp-row"):
            yield Label("Forecast bars", classes="kp-label")
            yield Input(str(kats_cfg.kronos_forecast_horizon), id="kp-horizon",
                        classes="kp-input", placeholder="24")

        with Horizontal(classes="kp-row"):
            yield Label("MC samples", classes="kp-label")
            yield Input(str(kats_cfg.kronos_mc_samples), id="kp-mc-samples",
                        classes="kp-input", placeholder="50")

        yield Static("[dim]Ctrl+S=apply & restart  Esc=close[/dim]", id="kp-hint")

    def set_status(self, text: str, style: str = "dim") -> None:
        try:
            self.query_one("#kp-status", Static).update(f"[{style}]{text}[/{style}]")
        except Exception:
            pass

    def sync_fields(self, cfg: dict) -> None:
        """Update widgets to match confirmed live settings after a restart."""
        if "model_size" in cfg:
            try:
                self.query_one("#kp-model-size", Select).value = cfg["model_size"]
            except Exception:
                pass
        if "device" in cfg:
            try:
                self.query_one("#kp-device", Select).value = cfg["device"]
            except Exception:
                pass
        int_fields = {
            "#kp-max-context": "max_context",
            "#kp-horizon":     "forecast_horizon",
            "#kp-mc-samples":  "mc_samples",
        }
        for widget_id, key in int_fields.items():
            if key in cfg:
                try:
                    self.query_one(widget_id, Input).value = str(cfg[key])
                except Exception:
                    pass

    def action_apply_settings(self) -> None:
        def _int(wid: str, label: str) -> int | None:
            raw = self.query_one(wid, Input).value.strip()
            if not raw:
                return None
            try:
                v = int(raw)
                if v <= 0:
                    raise ValueError
                return v
            except ValueError:
                self.set_status(f"⚠ {label} must be a positive integer", "yellow")
                return -1   # sentinel: abort

        cfg: dict = {}

        # Select widgets — value is always valid since options are fixed
        model_size = self.query_one("#kp-model-size", Select).value
        if model_size and model_size is not Select.BLANK:
            cfg["model_size"] = str(model_size)

        device = self.query_one("#kp-device", Select).value
        if device and device is not Select.BLANK:
            cfg["device"] = str(device)

        for wid, key, label in [
            ("#kp-max-context", "max_context",      "max context"),
            ("#kp-horizon",     "forecast_horizon",  "forecast bars"),
            ("#kp-mc-samples",  "mc_samples",        "MC samples"),
        ]:
            v = _int(wid, label)
            if v == -1:
                return   # validation error already shown
            if v is not None:
                cfg[key] = v

        if not cfg:
            self.set_status("nothing changed", "dim")
            return

        self.set_status("⟳ restarting Kronos…", "bold yellow")
        self.post_message(self.ApplySettings(cfg))

    def action_close_panel(self) -> None:
        self.post_message(self.ClosePanel())


# ── Key bindings bar (wrapping footer) ───────────────────────────────────────

class KeyBar(Static):
    """Two-row key bindings bar — wraps instead of scrolling."""

    DEFAULT_CSS = """
    KeyBar {
        dock: bottom;
        height: 2;
        background: $panel;
        color: $text;
        padding: 0 1;
    }
    """

    _ROWS: list[list[tuple[str, str]]] = [
        [("q", "Quit"), ("k", "Kill switch"), ("p", "Pause"), ("c", "Close…"), ("m", "Mode")],
        [("t", "Timeframe"), ("i", "Instruments"), ("x", "Kronos"), ("b", "Browser"), ("n", "Broker")],
    ]

    def render(self) -> Text:
        t = Text(no_wrap=True)
        for i, row in enumerate(self._ROWS):
            for key, label in row:
                t.append(f" {key} ", style="bold reverse")
                t.append(f" {label}  ")
            if i < len(self._ROWS) - 1:
                t.append("\n")
        return t


# ── CSS ────────────────────────────────────────────────────────────────────────

CSS = """
Screen {
    layers: default overlay;
}

StatusBar {
    dock: top;
    height: 1;
    background: $boost;
}

AccountPanel, RiskPanel {
    height: 100%;
}

.panel-title {
    text-style: bold;
    color: $accent;
    margin-bottom: 1;
}

/* Main content fills remaining space between docked top/bottom bars */
#main {
    height: 1fr;
}

#top-row {
    height: 14;
}

SignalPanel { margin-top: 1; }

/* Bottom split: positions TOP (fixed), event log BOTTOM (fills rest) */
#bottom-split {
    height: 1fr;
    margin-top: 1;
    layout: vertical;
}

#positions-container {
    height: 14;
    width: 100%;
    border: solid $primary;
    padding: 1 2;
}

PositionsTable {
    height: 1fr;
}

#event-log {
    height: 1fr;
    width: 100%;
    border: solid $primary;
    margin-top: 1;
}

/* Checkbox kill switch — red glow when active */
#kill-switch-toggle {
    margin-top: 1;
    height: auto;
    border: none;
    background: transparent;
}

#kill-switch-toggle.-on {
    color: $error;
    text-style: bold;
}

#kill-switch-toggle.-on .toggle--label {
    color: $error;
    text-style: bold;
}
"""


# ── Main App ───────────────────────────────────────────────────────────────────

class KronosTradeTUI(App):
    CSS = CSS

    BINDINGS = [
        Binding("q", "quit",              "Quit"),
        Binding("k", "kill_switch",       "Kill switch",  priority=True),
        Binding("p", "toggle_pause",      "Pause Kronos", priority=True),
        Binding("c", "toggle_close",      "Close…",       priority=True),
        Binding("m", "cycle_mode",        "Mode"),
        Binding("t", "cycle_timeframe",   "Timeframe"),
        Binding("i", "open_instruments",  "Instruments"),
        Binding("x", "open_kronos",       "Kronos settings"),
        Binding("r", "refresh",           "Refresh",      priority=True),
        Binding("b", "open_web",          "Browser"),
        Binding("n", "next_broker",       "Next broker"),
    ]

    _MODES      = ["scalping", "day", "swing"]
    _TIMEFRAMES = ["1m", "5m", "15m", "1h", "4h", "1d"]

    def __init__(self) -> None:
        super().__init__()
        self._http              = httpx.AsyncClient(base_url=API_BASE, timeout=5.0)
        self._kill_active       = False
        self._prices:           dict[str, float] = {}
        self._prev_prices:      dict[str, float] = {}
        self._positions:        list[dict] = []
        self._account_data:     dict = {}
        self._session_start     = _time.monotonic()
        self._ignore_next_kill_toggle = False
        self._available_brokers: list[str] = []
        self._active_broker:     str = ""
        self._trading_mode:      str = kats_cfg.trading_mode.value
        self._current_timeframe: str = kats_cfg.default_timeframe
        self._kronos_paused:     bool = False

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield StatusBar(id="status-bar")

        with Vertical(id="main"):
            yield PriceTicker(id="ticker-bar")
            with Horizontal(id="top-row"):
                yield AccountPanel(id="account-panel")
                yield RiskPanel(id="risk-panel")
            yield SignalPanel(id="signal-panel")
            with Vertical(id="bottom-split"):
                with Container(id="positions-container"):
                    yield Label("Open Positions", classes="panel-title")
                    yield PositionsTable(id="positions-table")
                yield RichLog(
                    id="event-log",
                    highlight=True, markup=True, wrap=True
                )

        yield InstrumentsPanel(id="instruments-panel")
        yield KronosPanel(id="kronos-panel")
        yield KeyBar()

    def on_mount(self) -> None:
        self._start_ws_listener()
        self._refresh_timer = self.set_interval(10, self._poll_state)
        self._uptime_timer  = self.set_interval(1,  self._tick_uptime)
        # Seed labels from config until first poll/WS event arrives
        self._update_risk_mode(self._trading_mode)
        self._update_timeframe_display(self._current_timeframe)
        self.title = "KATS"

    # ── Uptime ────────────────────────────────────────────────────────────────

    def _tick_uptime(self) -> None:
        e = int(_time.monotonic() - self._session_start)
        h, m, s = e // 3600, (e % 3600) // 60, e % 60
        self.query_one("#status-bar", StatusBar).uptime = f"{h:02d}:{m:02d}:{s:02d}"

    # ── Kill switch ───────────────────────────────────────────────────────────

    @on(Checkbox.Changed, "#kill-switch-toggle")
    async def _on_kill_toggle(self, event: Checkbox.Changed) -> None:
        if self._ignore_next_kill_toggle:
            self._ignore_next_kill_toggle = False
            self._kill_active = event.value
            return
        self._kill_active = event.value
        try:
            await self._http.post("/kill-switch", json={"engage": event.value})
        except Exception:
            pass
        self._log(f"Kill switch {'[red bold]ENGAGED[/]' if event.value else '[green]DISENGAGED[/]'}")

    async def action_kill_switch(self) -> None:
        cb = self.query_one("#kill-switch-toggle", Checkbox)
        cb.value = not cb.value

    async def action_toggle_pause(self) -> None:
        endpoint = "/kronos/resume" if self._kronos_paused else "/kronos/pause"
        try:
            await self._http.post(endpoint)
        except Exception as exc:
            self._log(f"[red]Kronos pause toggle failed: {exc}[/red]")

    # ── Close modal ───────────────────────────────────────────────────────────

    def action_toggle_close(self) -> None:
        if not self._positions:
            self._log("[dim]No open positions to close[/dim]")
            return
        self.push_screen(CloseModal(self._positions, self._http))

    # ── Other actions ─────────────────────────────────────────────────────────

    async def action_open_web(self) -> None:
        import webbrowser
        webbrowser.open("http://localhost:3000")

    async def action_next_broker(self) -> None:
        if not self._available_brokers:
            self._log("[dim]No broker info yet — press r to refresh[/dim]")
            return
        if len(self._available_brokers) < 2:
            self._log(f"[dim]Only one broker connected: {self._active_broker}[/dim]")
            return
        try:
            idx  = self._available_brokers.index(self._active_broker)
        except ValueError:
            idx  = -1
        next_broker = self._available_brokers[(idx + 1) % len(self._available_brokers)]
        try:
            await self._http.post("/broker", json={"broker": next_broker})
            self._active_broker = next_broker
            self.query_one("#status-bar", StatusBar).broker = next_broker.upper()
            self._log(f"[cyan]Broker → {next_broker.upper()}[/cyan]")
        except Exception as exc:
            self._log(f"[red]Broker switch failed: {exc}[/red]")

    async def action_cycle_mode(self) -> None:
        idx  = self._MODES.index(self._trading_mode) if self._trading_mode in self._MODES else 0
        next_mode = self._MODES[(idx + 1) % len(self._MODES)]
        try:
            await self._http.post("/mode", json={"mode": next_mode})
            self._trading_mode = next_mode
            self.query_one("#status-bar", StatusBar).mode = next_mode
            self._update_risk_mode(next_mode)
            self._log(f"[bold]Trading mode → {next_mode.upper()}[/bold]")
        except Exception as exc:
            self._log(f"[red]Mode switch failed: {exc}[/red]")

    async def action_cycle_timeframe(self) -> None:
        idx = self._TIMEFRAMES.index(self._current_timeframe) if self._current_timeframe in self._TIMEFRAMES else 3
        next_tf = self._TIMEFRAMES[(idx + 1) % len(self._TIMEFRAMES)]
        try:
            await self._http.post("/timeframe", json={"timeframe": next_tf})
            self._current_timeframe = next_tf
            self._update_timeframe_display(next_tf)
            self._log(f"[bold]Timeframe → {next_tf}[/bold]")
        except Exception as exc:
            self._log(f"[red]Timeframe switch failed: {exc}[/red]")

    def action_open_instruments(self) -> None:
        panel = self.query_one("#instruments-panel", InstrumentsPanel)
        if "active" in panel.classes:
            panel.remove_class("active")
        else:
            panel.add_class("active")
            if not panel._loaded:
                panel.load_symbols(self._http)
            self.set_timer(0.05, lambda: panel.query_one("#ip-filter", Input).focus())

    async def on_instruments_panel_save_selected(
        self, msg: InstrumentsPanel.SaveSelected
    ) -> None:
        self.query_one("#instruments-panel", InstrumentsPanel).remove_class("active")
        try:
            await self._http.post("/instruments", json={"instruments": msg.instruments})
            count = len(msg.instruments)
            self._log(
                f"[cyan]Instruments saved:[/cyan] {count} active "
                f"({', '.join(msg.instruments[:5])}{'…' if count > 5 else ''})"
            )
        except Exception as exc:
            self._log(f"[red]Instruments save failed: {exc}[/red]")

    def on_instruments_panel_close_panel(self, _: InstrumentsPanel.ClosePanel) -> None:
        self.query_one("#instruments-panel", InstrumentsPanel).remove_class("active")

    # ── Kronos panel ──────────────────────────────────────────────────────────

    def action_open_kronos(self) -> None:
        panel = self.query_one("#kronos-panel", KronosPanel)
        if "active" in panel.classes:
            panel.remove_class("active")
        else:
            panel.add_class("active")
            self.set_timer(0.05, lambda: panel.query_one("#kp-model-size", Select).focus())

    async def on_kronos_panel_apply_settings(self, msg: KronosPanel.ApplySettings) -> None:
        try:
            await self._http.post("/kronos/settings", json=msg.cfg)
            keys = ", ".join(f"{k}={v}" for k, v in msg.cfg.items())
            self._log(f"[bold magenta]Kronos restarting:[/bold magenta] {keys}")
        except Exception as exc:
            self._log(f"[red]Kronos settings apply failed: {exc}[/red]")
            self.query_one("#kronos-panel", KronosPanel).set_status(f"✗ {exc}", "red")

    def on_kronos_panel_close_panel(self, _: KronosPanel.ClosePanel) -> None:
        self.query_one("#kronos-panel", KronosPanel).remove_class("active")

    async def action_refresh(self) -> None:
        self._log("[dim]↻ refreshing…[/dim]")
        sb = self.query_one("#status-bar", StatusBar)
        orig_status = sb.status
        sb.status = "↻ SYNC"
        await self._poll_state()
        sb.status = orig_status
        self._log("[dim]refresh complete[/dim]")

    # ── State polling ─────────────────────────────────────────────────────────

    async def _poll_state(self) -> None:
        try:
            state_resp, account_resp, brokers_resp, mode_resp, tf_resp = await asyncio.gather(
                self._http.get("/state"),
                self._http.get("/account"),
                self._http.get("/brokers"),
                self._http.get("/mode"),
                self._http.get("/timeframe"),
                return_exceptions=True,
            )
            if isinstance(state_resp, httpx.Response):
                self._apply_state(state_resp.json())
            if isinstance(account_resp, httpx.Response) and account_resp.status_code == 200:
                self._apply_account(account_resp.json())
            if isinstance(brokers_resp, httpx.Response) and brokers_resp.status_code == 200:
                self._apply_brokers(brokers_resp.json())
            if isinstance(mode_resp, httpx.Response) and mode_resp.status_code == 200:
                m = mode_resp.json().get("mode", self._trading_mode)
                self._trading_mode = m
                self.query_one("#status-bar", StatusBar).mode = m
                self._update_risk_mode(m)
            if isinstance(tf_resp, httpx.Response) and tf_resp.status_code == 200:
                tf = tf_resp.json().get("timeframe", self._current_timeframe)
                self._current_timeframe = tf
                self._update_timeframe_display(tf)
        except Exception:
            pass

    def _apply_brokers(self, data: dict) -> None:
        self._available_brokers = data.get("available", [])
        active = data.get("active", "")
        if active and active != self._active_broker:
            self._active_broker = active
            self.query_one("#status-bar", StatusBar).broker = active.upper()

    def _apply_state(self, state: dict) -> None:
        sb = self.query_one("#status-bar", StatusBar)
        sb.status = "● LIVE" if state.get("running") else "● STOPPED"
        sb.kronos = f"Kronos: {'✓' if state.get('kronos_loaded') else 'loading…'}"
        paused = state.get("kronos_paused", False)
        self._kronos_paused = paused
        sb.paused = paused
        self._update_kronos_state(paused)
        # Mode and timeframe come through /state so they persist on reload
        # (same pattern as kill_switch — avoids relying solely on WS startup broadcast)
        if state.get("trading_mode"):
            m = state["trading_mode"]
            if m != self._trading_mode:
                self._trading_mode = m
                sb.mode = m
                self._update_risk_mode(m)
        if state.get("timeframe"):
            tf = state["timeframe"]
            if tf != self._current_timeframe:
                self._current_timeframe = tf
                self._update_timeframe_display(tf)
        self._positions = list(state.get("positions", []) or [])
        self._sync_positions_ui()
        self.query_one("#risk-panel", RiskPanel).update(state)

        if state.get("last_signal"):
            self.query_one("#signal-panel", SignalPanel).update(state["last_signal"])

        self._sync_kill_switch(state.get("kill_switch", False))
        if state.get("daily_pnl") is not None and self._account_data:
            self._apply_account(self._account_data | {"daily_pnl": state.get("daily_pnl", 0)})

    def _apply_account(self, account: dict) -> None:
        self._account_data = dict(account)
        unrealized = account.get("unrealized_pnl")
        if unrealized is None:
            unrealized = self._positions_unrealized()
        elif self._positions:
            unrealized = self._positions_unrealized()

        data = dict(account)
        data["unrealized_pnl"] = unrealized
        self.query_one("#account-panel", AccountPanel).update(data)

    def _positions_unrealized(self) -> float:
        return sum((p.get("unrealized_pnl", 0) or 0) for p in self._positions)

    def _sync_positions_ui(self) -> None:
        self.query_one("#positions-table", PositionsTable).refresh_positions(self._positions)
        if self._account_data:
            self._apply_account(self._account_data)

    def _update_risk_mode(self, mode: str) -> None:
        try:
            color = {"scalping": "yellow", "day": "blue", "swing": "green"}.get(mode, "dim")
            self.query_one("#r-mode").update(
                Text.from_markup(f"Mode:        [{color}]{mode}[/{color}]")
            )
        except Exception:
            pass

    def _update_timeframe_display(self, tf: str) -> None:
        try:
            sb = self.query_one("#status-bar", StatusBar)
            sb.timeframe = tf
            self.query_one("#r-tf").update(
                Text.from_markup(f"Timeframe:   [white]{tf}[/white]")
            )
        except Exception:
            pass

    def _update_kronos_state(self, paused: bool) -> None:
        try:
            if paused:
                self.query_one("#r-kronos").update(
                    Text.from_markup("Kronos:      [yellow bold]⏸ paused[/yellow bold]")
                )
            else:
                self.query_one("#r-kronos").update(
                    Text.from_markup("Kronos:      [green]▶ running[/green]")
                )
        except Exception:
            pass

    def _sync_kill_switch(self, active: bool) -> None:
        self._kill_active = active
        cb = self.query_one("#kill-switch-toggle", Checkbox)
        if cb.value == active:
            return
        self._ignore_next_kill_toggle = True
        cb.value = active

    # ── WebSocket ─────────────────────────────────────────────────────────────

    @work(exclusive=True, thread=False)
    async def _start_ws_listener(self) -> None:
        first = True
        while True:
            try:
                async with websockets.connect(
                    WS_URL,
                    ping_interval=20,
                    ping_timeout=10,
                ) as ws:
                    msg = "Connected" if first else "Reconnected"
                    self._log(f"[green]{msg} to system[/green]")
                    first = False
                    await self._poll_state()
                    async for raw in ws:
                        try:
                            self._handle_event(json.loads(raw))
                        except json.JSONDecodeError:
                            pass
            except Exception:
                self.query_one("#status-bar", StatusBar).status = "● RECONNECTING"
                self._log("[dim]Disconnected — retrying…[/dim]")
                await asyncio.sleep(3)

    def _handle_event(self, event: dict) -> None:
        kind = event.get("type", "unknown")
        data = event.get("data", {})

        if kind == "prices":
            self._prev_prices = dict(self._prices)
            self._prices      = data
            self.query_one("#ticker-bar", PriceTicker).update_prices(
                self._prices, self._prev_prices
            )

        elif kind == "positions":
            self._positions = list(data or [])
            self._sync_positions_ui()
            self.query_one("#r-pos").update(
                f"Positions:   {len(self._positions)} / {kats_cfg.max_concurrent_positions}"
            )

        elif kind == "account":
            self._apply_account(data)

        elif kind == "signal":
            sym   = data.get("symbol", "?")
            dirn  = data.get("direction", "").upper()
            conf  = data.get("confidence", 0) * 100
            color = "green" if dirn == "LONG" else "red" if dirn == "SHORT" else "dim"
            self._log(f"[bold]{sym}[/bold] [{color}]{dirn}[/{color}] conf={conf:.1f}%")
            self.query_one("#signal-panel", SignalPanel).update(data)

        elif kind == "order":
            sym  = data.get("symbol", "?")
            side = data.get("side", "").upper()
            qty  = data.get("quantity", 0)
            ep   = data.get("entry_price", 0)
            sl   = data.get("stop_loss", 0)
            tp   = data.get("take_profit", 0)
            self._log(
                f"[cyan]ORDER[/cyan] {sym} {side} qty={qty:.4f} @ {ep:.4f} "
                f"sl={sl:.4f} tp={tp:.4f}"
            )

        elif kind == "exit":
            sym    = data.get("symbol", "?")
            reason = data.get("reason", "?")
            price  = data.get("price", 0)
            color  = "green" if reason == "take_profit" else "red"
            label  = "TP HIT ✓" if reason == "take_profit" else "SL HIT ✗"
            self._log(f"[{color}]{label}[/{color}] {sym} @ {price:.4f}")

        elif kind == "risk_reject":
            self._log(f"[red]RISK REJECT:[/red] {data.get('reason', '?')}")

        elif kind == "broker_switch":
            broker = data.get("broker", "?").upper()
            self._active_broker = data.get("broker", self._active_broker)
            self.query_one("#status-bar", StatusBar).broker = broker
            self._log(f"[cyan]Broker switched → {broker}[/cyan]")

        elif kind == "mode_switch":
            mode = data.get("mode", self._trading_mode)
            self._trading_mode = mode
            self.query_one("#status-bar", StatusBar).mode = mode
            self._update_risk_mode(mode)
            self._log(f"[bold]Trading mode → {mode.upper()}[/bold]")

        elif kind == "kill_switch":
            active = data.get("active", False)
            self._sync_kill_switch(active)
            self._log(
                f"[red bold]⚠ Kill switch ENGAGED[/red bold]" if active
                else "[green]Kill switch disengaged[/green]"
            )

        elif kind == "kronos_pause":
            paused = data.get("paused", False)
            self._kronos_paused = paused
            sb = self.query_one("#status-bar", StatusBar)
            sb.paused = paused
            self._update_kronos_state(paused)
            if paused:
                self._log("[yellow bold]⏸ Kronos PAUSED — no new signals[/yellow bold]")
            else:
                self._log("[green bold]▶ Kronos RESUMED[/green bold]")

        elif kind == "timeframe_switch":
            tf = data.get("timeframe", self._current_timeframe)
            self._current_timeframe = tf
            self._update_timeframe_display(tf)
            self._log(f"[bold]Timeframe → {tf}[/bold]")

        elif kind == "risk_halt":
            daily = data.get("daily_halted",    False)
            dd    = data.get("drawdown_halted",  False)
            # RiskPanel.update() expects the same keys as the /state response
            self.query_one("#risk-panel", RiskPanel).update({
                "daily_loss_halted": daily,
                "drawdown_halted":   dd,
            })
            if daily:
                self._log("[red bold]⚠ DAILY LOSS HALT — trading suspended until next day[/red bold]")
            if dd:
                self._log("[red bold]⚠ DRAWDOWN HALT — trading suspended until max_drawdown_pct is raised in config[/red bold]")

        elif kind == "kronos_restart":
            status   = data.get("status", "")
            kp       = self.query_one("#kronos-panel", KronosPanel)
            if status == "restarting":
                sb.kronos = "Kronos: restarting…"
                kp.set_status("⟳ loading model…", "bold yellow")
            elif status == "ready":
                cfg = data.get("settings", {})
                size   = cfg.get("model_size", "?")
                device = cfg.get("device", "?")
                sb.kronos = f"Kronos: {size}@{device}"
                kp.set_status("✓ ready", "bold green")
                kp.sync_fields(cfg)
                self._log(
                    f"[bold magenta]Kronos ready:[/bold magenta] "
                    f"model={size} device={device} "
                    f"horizon={cfg.get('forecast_horizon')} mc={cfg.get('mc_samples')}"
                )
            elif status == "error":
                err = data.get("message", "unknown error")
                sb.kronos = "Kronos: ERROR"
                kp.set_status(f"✗ {err}", "red")
                self._log(f"[red bold]Kronos restart failed:[/red bold] {err}")

        elif kind == "standby":
            nxt       = event.get("next_open")   # top-level, not nested under "data"
            countdown = event.get("countdown", "")
            sb        = self.query_one("#status-bar", StatusBar)
            if nxt:
                sb.standby = f"{nxt} ({countdown})"
                sb.status  = "⏸ STANDBY"
                self._log(f"[yellow]⏸ STANDBY — next window: {nxt} ({countdown})[/yellow]")
            else:
                sb.standby = ""
                sb.status  = "● LIVE"
                self._log("[green bold]▶ Trading window opened — resuming[/green bold]")

        elif kind == "instruments_update":
            instrs = data.get("instruments", [])
            self._log(
                f"[cyan]Instruments updated:[/cyan] "
                f"{', '.join(instrs[:5])}{'…' if len(instrs) > 5 else ''}"
            )
            # Prune stale symbols from the ticker immediately
            active = set(instrs)
            for sym in [s for s in self._prices if s not in active]:
                self._prices.pop(sym, None)
                self._prev_prices.pop(sym, None)
            self.query_one("#ticker-bar", PriceTicker).update_prices(
                self._prices, self._prev_prices
            )

        elif kind == "shutdown":
            self._log("[red]System shutdown — closing dashboard[/red]")
            self.set_timer(2.5, self.exit)

    def _log(self, msg: str) -> None:
        ts = datetime.now(tz=timezone.utc).strftime("%H:%M:%S")
        self.query_one("#event-log", RichLog).write(f"[dim]{ts}[/dim] {msg}")


# ── Demo ───────────────────────────────────────────────────────────────────────

DEMO_STATE = {
    "running": True, "kronos_loaded": True, "uptime_seconds": 3723,
    "daily_loss_halted": False, "drawdown_halted": False, "kill_switch": False,
    "kronos_paused": False, "trading_mode": "day", "timeframe": "1h",
    "positions": [
        {"symbol": "BTCUSD", "direction": "long",  "quantity": 0.28,
         "entry_price": 70875.49, "current_price": 71200.0,
         "stop_loss": 70619.85, "take_profit": 71386.78,
         "unrealized_pnl": 91.20, "broker": "alpaca"},
    ],
    "equity": 100000.0, "cash": 80000.0, "daily_pnl": 91.20,
    "unrealized_pnl": 91.20,
    "last_signal": {
        "symbol": "BTCUSD", "generated_at": "2026-04-12T10:00:00",
        "direction": "long", "confidence": 0.92, "entry_price": 70875.49,
        "volatility_forecast": 631.7,
        "forecast_mean":  [70900 + i * 20 for i in range(24)],
        "forecast_lower": [70800 + i * 18 for i in range(24)],
        "forecast_upper": [71000 + i * 22 for i in range(24)],
    },
}
DEMO_PRICES = {"BTCUSD": 71200.0, "ETHUSD": 2198.0}
DEMO_EVENTS = [
    {"type": "signal",      "data": DEMO_STATE["last_signal"]},
    {"type": "order",       "data": {"symbol": "BTCUSD", "side": "buy",
                                     "quantity": 0.28, "entry_price": 70875.49,
                                     "stop_loss": 70619.85, "take_profit": 71386.78}},
    {"type": "risk_reject", "data": {"reason": "position already open for BTCUSD"}},
    {"type": "exit",        "data": {"symbol": "BTCUSD", "reason": "take_profit",
                                     "price": 71386.78}},
]


class KronosTradeDemoTUI(KronosTradeTUI):
    def on_mount(self) -> None:
        super().on_mount()  # starts uptime ticker and clock
        self._apply_state(DEMO_STATE)
        self.query_one("#account-panel", AccountPanel).update(DEMO_STATE)
        self.query_one("#ticker-bar",    PriceTicker).update_prices(DEMO_PRICES, {})
        self._demo_index = 0
        self.set_interval(1.4, self._replay)

    def _replay(self) -> None:
        self._handle_event(DEMO_EVENTS[self._demo_index % len(DEMO_EVENTS)])
        self._demo_index += 1


# ── Entry ──────────────────────────────────────────────────────────────────────

def run_dashboard(demo: bool = False) -> None:
    (KronosTradeDemoTUI if demo else KronosTradeTUI)().run()

if __name__ == "__main__":
    import sys
    run_dashboard(demo="--demo" in sys.argv)

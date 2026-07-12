"""
scripts/backtest.py
Walk-forward backtest using Kronos predictions on historical OHLCV data.

Usage:
  python scripts/backtest.py --symbol BTCUSD --bars 1000 --horizon 24
  python scripts/backtest.py --symbol NQ --bars 2000 --tf 1h --plot

Methodology:
  For each bar t in [warmup..N]:
    1. Feed bars [t-context..t] to KronosPredictor
    2. Generate signal (direction + confidence)
    3. Simulate trade entry/exit using the actual next `horizon` bars
    4. Record PnL and track equity curve
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import typer
from loguru import logger
from rich.console import Console
from rich.table import Table

sys.path.insert(0, str(Path(__file__).parents[1]))

from kronos_trade.config import kats_cfg, settings
from kronos_trade.data.feeds.alpaca_feed import AlpacaBarFeed
from kronos_trade.kronos.predictor import KronosEngine
from kronos_trade.models import Direction

app  = typer.Typer()
console = Console()


async def _run_backtest(
    symbol: str,
    n_bars: int,
    context: int,
    horizon: int,
    confidence_threshold: float,
    rr_ratio: float,
) -> dict:
    """Core backtest coroutine."""

    # ── Load historical data ──────────────────────────────────────────────────
    logger.info(f"[backtest] fetching {n_bars} bars for {symbol}…")
    feed = AlpacaBarFeed(symbols=[symbol], timeframe=kats_cfg.default_timeframe)
    bars = await feed.fetch_history(symbol, n_bars=n_bars)

    if len(bars) < context + horizon + 10:
        logger.error(f"Not enough bars: got {len(bars)}, need {context + horizon + 10}")
        return {}

    df_full = pd.DataFrame([{
        "open":      b.open,
        "high":      b.high,
        "low":       b.low,
        "close":     b.close,
        "volume":    b.volume,
        "timestamp": b.timestamp,
    } for b in bars]).set_index("timestamp")

    # ── Load Kronos ───────────────────────────────────────────────────────────
    logger.info("[backtest] loading Kronos model…")
    engine = KronosEngine()
    await engine.load()

    # ── Walk-forward simulation ───────────────────────────────────────────────
    equity         = 10_000.0
    equity_curve   = [equity]
    trade_log      = []
    wins = losses  = skips = 0

    logger.info(f"[backtest] running walk-forward | steps={len(df_full) - context - horizon}")

    for t in range(context, len(df_full) - horizon):
        window_df = df_full.iloc[t - context : t][["open", "high", "low", "close", "volume"]]
        window_ts = pd.Series(window_df.index)

        try:
            forecast = await engine.predict(window_df.reset_index(drop=True), window_ts, horizon)
        except Exception as exc:
            logger.debug(f"[backtest] t={t} forecast error: {exc}")
            skips += 1
            continue

        mean_fc  = forecast["mean"]
        samples  = forecast["samples"]
        entry    = float(df_full.iloc[t]["close"])

        # Direction + confidence
        mid = horizon // 2
        bullish_frac = float(np.mean(samples[:, mid] > entry))
        bearish_frac = 1.0 - bullish_frac

        if bullish_frac >= confidence_threshold:
            direction  = Direction.LONG
            confidence = bullish_frac
        elif bearish_frac >= confidence_threshold:
            direction  = Direction.SHORT
            confidence = bearish_frac
        else:
            skips += 1
            continue

        # Compute ATR for stop sizing
        recent = df_full.iloc[max(0, t-14) : t]
        trs    = []
        for i in range(1, len(recent)):
            h, l, pc = recent.iloc[i]["high"], recent.iloc[i]["low"], recent.iloc[i-1]["close"]
            trs.append(max(h-l, abs(h-pc), abs(l-pc)))
        atr = sum(trs[-14:]) / len(trs[-14:]) if trs else entry * 0.001

        # Bracket levels
        if direction == Direction.LONG:
            stop   = entry - atr
            target = entry + atr * rr_ratio
        else:
            stop   = entry + atr
            target = entry - atr * rr_ratio

        risk_per_unit = abs(entry - stop)
        risk_dollars  = equity * kats_cfg.max_position_risk_pct / 100
        qty           = risk_dollars / risk_per_unit if risk_per_unit > 0 else 0

        if qty <= 0:
            skips += 1
            continue

        # Simulate the trade over the next `horizon` bars
        future_bars = df_full.iloc[t + 1 : t + 1 + horizon]
        trade_pnl   = 0.0
        exit_reason = "timeout"

        for _, fbar in future_bars.iterrows():
            if direction == Direction.LONG:
                if fbar["low"] <= stop:
                    trade_pnl   = (stop - entry) * qty
                    exit_reason = "stop_loss"
                    break
                if fbar["high"] >= target:
                    trade_pnl   = (target - entry) * qty
                    exit_reason = "take_profit"
                    break
            else:
                if fbar["high"] >= stop:
                    trade_pnl   = (entry - stop) * qty
                    exit_reason = "stop_loss"
                    break
                if fbar["low"] <= target:
                    trade_pnl   = (entry - target) * qty
                    exit_reason = "take_profit"
                    break

        # Timeout: use last bar close
        if exit_reason == "timeout":
            exit_price = float(future_bars.iloc[-1]["close"])
            if direction == Direction.LONG:
                trade_pnl = (exit_price - entry) * qty
            else:
                trade_pnl = (entry - exit_price) * qty

        equity += trade_pnl
        equity_curve.append(equity)

        trade_log.append({
            "t":          t,
            "direction":  direction.value,
            "confidence": confidence,
            "entry":      entry,
            "pnl":        trade_pnl,
            "exit":       exit_reason,
            "equity":     equity,
        })

        if trade_pnl >= 0:
            wins += 1
        else:
            losses += 1

    # ── Metrics ───────────────────────────────────────────────────────────────
    n_trades  = wins + losses
    win_rate  = wins / n_trades if n_trades else 0
    total_pnl = equity - 10_000.0
    returns   = np.array([t["pnl"] for t in trade_log])
    sharpe    = (returns.mean() / returns.std() * np.sqrt(252)) if len(returns) > 1 and returns.std() > 0 else 0

    ec        = np.array(equity_curve)
    peak      = np.maximum.accumulate(ec)
    dd        = (peak - ec) / peak
    max_dd    = float(dd.max()) * 100

    return {
        "symbol":    symbol,
        "n_bars":    n_bars,
        "n_trades":  n_trades,
        "wins":      wins,
        "losses":    losses,
        "skips":     skips,
        "win_rate":  win_rate,
        "total_pnl": total_pnl,
        "sharpe":    sharpe,
        "max_dd_pct": max_dd,
        "final_equity": equity,
        "equity_curve": equity_curve,
        "trade_log": trade_log,
    }


def _print_results(results: dict, plot: bool) -> None:
    console.print()
    t = Table(title=f"[bold]Backtest Results — {results['symbol']}[/bold]", show_header=False)
    t.add_column("Metric", style="dim")
    t.add_column("Value", style="bold")

    wr    = results["win_rate"]
    pnl   = results["total_pnl"]
    sharpe = results["sharpe"]

    t.add_row("Bars tested",       str(results["n_bars"]))
    t.add_row("Trades taken",      str(results["n_trades"]))
    t.add_row("Signals skipped",   str(results["skips"]))
    t.add_row("Win rate",          f"[green]{wr:.1%}[/green]" if wr >= 0.5 else f"[red]{wr:.1%}[/red]")
    t.add_row("W / L",             f"{results['wins']} / {results['losses']}")
    t.add_row("Total P&L",         f"[green]+${pnl:,.2f}[/green]" if pnl >= 0 else f"[red]-${abs(pnl):,.2f}[/red]")
    t.add_row("Sharpe ratio",      f"{sharpe:.2f}")
    t.add_row("Max drawdown",      f"[red]{results['max_dd_pct']:.1f}%[/red]")
    t.add_row("Final equity",      f"${results['final_equity']:,.2f}")
    console.print(t)

    if plot and results.get("equity_curve"):
        try:
            import plotext as plt
            plt.clear_figure()
            plt.plot(results["equity_curve"], color="green")
            plt.title(f"{results['symbol']} equity curve")
            plt.xlabel("Bar")
            plt.ylabel("Equity ($)")
            plt.show()
        except ImportError:
            console.print("[dim]Install plotext for equity curve: pip install plotext[/dim]")


@app.command()
def main(
    symbol: str = typer.Option("BTCUSD", "--symbol", "-s"),
    bars:   int = typer.Option(1000,     "--bars",   "-n"),
    horizon:int = typer.Option(24,       "--horizon"),
    tf:     str = typer.Option("1h",     "--tf"),
    confidence: float = typer.Option(0.60, "--confidence", "-c"),
    rr:     float = typer.Option(2.0,    "--rr", help="Reward:risk ratio"),
    plot:   bool  = typer.Option(False,  "--plot", "-p", help="Plot equity curve"),
) -> None:
    """Run a walk-forward Kronos backtest on historical data."""
    from loguru import logger
    logger.remove()
    logger.add(sys.stderr, level="INFO", format="{message}")

    kats_cfg.default_timeframe = tf

    results = asyncio.run(_run_backtest(
        symbol=symbol,
        n_bars=bars,
        context=settings.kronos_max_context,
        horizon=horizon,
        confidence_threshold=confidence,
        rr_ratio=rr,
    ))

    if results:
        _print_results(results, plot=plot)


if __name__ == "__main__":
    app()

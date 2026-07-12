"""
KronosTrade — main entry point.

Starts:
  1. Data pipeline (feeds)
  2. Execution router (Kronos + strategy + risk + brokers)
  3. FastAPI server  (REST + WebSocket)
  4. Textual TUI     (optional, pass --tui flag)

Usage:
  python scripts/run_system.py
  python scripts/run_system.py --tui           # launch TUI in same terminal
  python scripts/run_system.py --paper         # force paper trading
  python scripts/run_system.py --symbols NQ,MNQ --broker ninjatrader
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import multiprocessing
import os as _os
import signal
import socket
import sys
import warnings
from pathlib import Path

# ── Auto-unset shell env vars that would shadow .env secrets ──────────────────
# pydantic-settings gives shell env vars higher priority than .env files.
# Non-secret config (mode, timeframe, instruments, risk params) now lives in
# kats_config.json and is no longer affected by shell env. Only clear the
# remaining secrets and Kronos model settings that must come from .env.
_ENV_KEYS_TO_CLEAR = [
    # Broker credentials — clear shell overrides so .env is always the
    # source of truth when API keys are rotated between accounts.
    "ALPACA_API_KEY",
    "ALPACA_SECRET_KEY",
    "DATABENTO_API_KEY",
    "OANDA_API_TOKEN",
    "OANDA_ACCOUNT_ID",
    "OANDA_PRACTICE",
    # Kronos model config stays in .env; clear to prevent shell overrides.
    "KRONOS_MODEL_SIZE",
    "KRONOS_DEVICE",
    "KRONOS_MAX_CONTEXT",
    "KRONOS_FORECAST_HORIZON",
    "KRONOS_MC_SAMPLES",
]
_cleared_env: dict[str, str] = {}
for _k in _ENV_KEYS_TO_CLEAR:
    if _k in _os.environ:
        _cleared_env[_k] = _os.environ.pop(_k)

# FastAPI 0.111.x uses asyncio.iscoroutinefunction which is deprecated in Python 3.12+
# and slated for removal in 3.16. Suppress until FastAPI is upgraded to >=0.112.
warnings.filterwarnings(
    "ignore",
    message="'asyncio.iscoroutinefunction' is deprecated",
    category=DeprecationWarning,
    module=r"fastapi\.routing",
)

import typer
import uvicorn
from loguru import logger

from kronos_trade.execution.brokers.base import BrokerAdapter

# ── ensure project root is in path ───────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parents[1]))

from kronos_trade.config import kats_cfg, settings, _ENV_FILE
from kronos_trade.data.feeds.alpaca_feed import AlpacaBarFeed, AlpacaPriceFeed
from kronos_trade.data.feeds.databento_feed import DatabentofFeed
from kronos_trade.data.feeds.oanda_feed import OANDABarFeed, OANDAPriceFeed
from kronos_trade.data.pipeline import DataPipeline
from kronos_trade.execution.brokers.alpaca import AlpacaAdapter
from kronos_trade.execution.brokers.oanda import OANDAAdapter
from kronos_trade.execution.brokers.ninjatrader import NinjaTraderAdapter
from kronos_trade.execution.router import ExecutionRouter
from kronos_trade.models import BrokerName
from kronos_trade.store.db import TradeStore

app = typer.Typer(add_completion=False)


def _configure_logging() -> None:
    import shutil
    import textwrap as _textwrap

    Path("logs").mkdir(exist_ok=True)
    logger.remove()

    # ── Custom level icons + colours ──────────────────────────────────────────
    logger.level("TRACE",    color="<dim><white>",        icon="·")
    logger.level("DEBUG",    color="<dim><cyan>",         icon="·")
    logger.level("INFO",     color="<bold><white>",       icon="ℹ")
    logger.level("SUCCESS",  color="<bold><green>",       icon="✓")
    logger.level("WARNING",  color="<bold><yellow>",      icon="⚠")
    logger.level("ERROR",    color="<bold><red>",         icon="✗")
    logger.level("CRITICAL", color="<bold><white><red>",  icon="☠")

    # Visual prefix width (no ANSI): "HH:MM:SS.mmm │ X LEVEL    │ "
    #   12 (time) + 3 ( │ ) + 1 (icon) + 1 (sp) + 8 (level:<8) + 3 ( │ ) = 28
    _PREFIX_COLS = 28

    def _console_fmt(record: dict) -> str:
        """
        Format function that pre-wraps the message at terminal width so
        continuation lines are indented to align with the message start,
        giving a clean hanging-indent look regardless of terminal size.
        """
        cols      = shutil.get_terminal_size((120, 40)).columns
        msg_width = max(cols - _PREFIX_COLS, 20)
        pad       = " " * _PREFIX_COLS

        parts: list[str] = []
        for i, line in enumerate(record["message"].split("\n")):
            chunks = _textwrap.wrap(
                line,
                width=msg_width,
                break_long_words=True,
                break_on_hyphens=False,
            ) or [""]
            # First segment of each input line — indent only for lines after line 0
            parts.append(("" if i == 0 else pad) + chunks[0])
            for chunk in chunks[1:]:
                parts.append(pad + chunk)

        # Store wrapped text in extra so it survives loguru's format_map.
        # Escape braces to prevent format_map from misinterpreting message content.
        record["extra"]["_msg"] = (
            "\n".join(parts).replace("{", "{{").replace("}", "}}")
        )
        return (
            "<dim>{time:HH:mm:ss.SSS}</dim>"
            " <dim>│</dim> "
            "<level>{level.icon} {level:<8}</level>"
            " <dim>│</dim> "
            "{extra[_msg]}\n"
        )

    _FILE_FMT = (
        "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<8} | "
        "{name}:{function}:{line} | {message}"
    )

    logger.add(
        sys.stderr,
        level=settings.log_level,
        format=_console_fmt,
        colorize=True,
    )
    logger.add(
        settings.log_file,
        level="DEBUG",
        format=_FILE_FMT,
        rotation="50 MB",
        retention="14 days",
        compression="gz",
        colorize=False,
    )

def _port_is_free(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("localhost", port)) != 0

async def _run_async(
    symbols: list[str],
    broker_name: BrokerName,
    enable_databento: bool,
    enable_alpaca: bool,
) -> None:
    """Main async coroutine — runs until SIGINT."""
    logger.info(f"[main] loading config from {_ENV_FILE} (exists={_ENV_FILE.exists()})")

    if settings.redis_available:
        logger.info("[main] Redis connected")
    else:
        logger.warning("[main] Redis unavailable - bar cache disabled (system will still run)")

    executor = concurrent.futures.ThreadPoolExecutor(
        max_workers=4,
        thread_name_prefix="kronos-worker"
    )
    loop = asyncio.get_event_loop()
    loop.set_default_executor(executor)

    # ── Database ──────────────────────────────────────────────────────────────
    store = TradeStore()
    await store.init()   # runs one-time schema migrations internally

    # ── Brokers ───────────────────────────────────────────────────────────────
    brokers: list[BrokerAdapter] = []
    price_feed = None

    if broker_name == BrokerName.ALPACA:
        brokers.append(AlpacaAdapter())
    elif broker_name == BrokerName.NINJATRADER:
        brokers.append(NinjaTraderAdapter())
    elif broker_name == BrokerName.OANDA:
        brokers.append(OANDAAdapter())

    # Connect brokers first so we can query their tradeable symbol lists
    primary = brokers[0] if brokers else None
    if primary: await primary.connect()

    # ── Filter symbols to only what the primary broker can actually trade ──────
    if primary and primary.supported_symbols:
        unsupported = [s for s in symbols if s not in primary.supported_symbols]
        if unsupported:
            logger.warning(
                f"[main] removing non-tradeable symbols on "
                f"{broker_name.value}: {unsupported}"
            )
        symbols = [s for s in symbols if s in primary.supported_symbols]

    if not symbols:
        logger.warning(
            "[main] no instruments configured — system will start without trading. "
            "Add instruments via the TUI Instruments panel (press 'i') or set them "
            "in kats_config.json."
        )

    logger.info(f"[main] trading symbols: {symbols}")

    # ── Data feeds ────────────────────────────────────────────────────────────
    feeds = []

    if broker_name == BrokerName.OANDA:
        # OANDA path: candles from REST polling, prices from SSE stream
        oanda_bar_feed   = OANDABarFeed(symbols=symbols, timeframe=kats_cfg.default_timeframe)
        oanda_price_feed = OANDAPriceFeed(symbols=symbols, poll_interval=5.0)
        feeds.append(oanda_bar_feed)
        price_feed = oanda_price_feed
    else:
        # Alpaca / default path
        alpaca_bar_feed   = AlpacaBarFeed(symbols=symbols, timeframe=kats_cfg.default_timeframe)
        alpaca_price_feed = AlpacaPriceFeed(symbols=symbols, poll_interval=10.0)
        feeds.append(alpaca_bar_feed)
        price_feed = alpaca_price_feed

        if enable_databento and settings.databento_api_key:
            db_syms = [s for s in symbols if s in {"NQ", "MNQ", "ES", "XAUUSD"}]
            if db_syms:
                feeds.append(DatabentofFeed(symbols=db_syms, timeframe=kats_cfg.default_timeframe))

    if not feeds:
        logger.error("[main] No data feeds configured — check API keys / bridge URL")
        return

    pipeline = DataPipeline(feeds=feeds)

    # ── Execution router ──────────────────────────────────────────────────────
    router = ExecutionRouter(
        pipeline       = pipeline,
        brokers        = brokers,
        symbols        = symbols,
        timeframe      = kats_cfg.default_timeframe,
        primary_broker = broker_name,
        price_feed     = price_feed,
        store          = store,
    )

    if price_feed:
        await price_feed.start()

    # ── FastAPI ───────────────────────────────────────────────────────────────
    from kronos_trade.api.main import create_app

    fastapi_app = create_app(router_instance=router, store_instance=store)
    uvicorn_config = uvicorn.Config(
        fastapi_app,
        host=settings.api_host,
        port=settings.api_port,
        log_level="warning",
        access_log=False,
    )
    if not _port_is_free(settings.api_port):
        logger.warning(
            f"[main] port {settings.api_port} already in use - "
            f"kill with: lsof -ti :{settings.api_port} | xargs kill -9"
        )
        logger.warning("[main] continuing without API server")
        uvicorn_server = None
    else:
        uvicorn_server = uvicorn.Server(uvicorn_config)

    # ── Startup sequence ──────────────────────────────────────────────────────
    env_note = ""
    if broker_name == BrokerName.OANDA:
        env_note = f" [{'PRACTICE' if settings.oanda_practice else 'LIVE'}]"
    logger.info("=" * 60)
    logger.info("  KronosTrade starting up")
    logger.info(f"  Symbols:   {symbols}")
    logger.info(f"  Timeframe: {kats_cfg.default_timeframe}")
    logger.info(f"  Broker:    {broker_name}{env_note}")
    logger.info(f"  API:       http://{settings.api_host}:{settings.api_port}")
    logger.info(f"  Dashboard: http://localhost:3000  (cd apps/web && npm run dev)")
    logger.info("=" * 60)

    await pipeline.start()
    await router.start()

    # Run FastAPI + router concurrently
    tasks = [
        asyncio.create_task(_keepalive(router, store), name="keepalive"),
    ]

    if uvicorn_server:
        tasks.append(asyncio.create_task(uvicorn_server.serve(), name="api-server"))

    stop_event = asyncio.Event()

    def _sigint_handler(*_):
        if not stop_event.is_set():
            logger.warning("[main] SIGINT received - shutting down")
            stop_event.set()

    loop.add_signal_handler(signal.SIGINT,  _sigint_handler)
    loop.add_signal_handler(signal.SIGTERM, _sigint_handler)

    await stop_event.wait()


    # ── Graceful shutdown ─────────────────────────────────────────────────────
    logger.info("[main] shutting down...")
    if uvicorn_server:
        uvicorn_server.should_exit = True
    for t in tasks: t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    if price_feed:
        await price_feed.stop()
    await router.stop()
    await pipeline.stop()
    await store.close()
    logger.success("[main] shutdown complete")


def _broker_compound_key(broker: BrokerName) -> str:
    """Return a compound broker+env key so live and demo/paper accounts are stored separately."""
    if broker == BrokerName.OANDA:
        return f"oanda_{'practice' if settings.oanda_practice else 'live'}"
    if broker == BrokerName.ALPACA:
        return f"alpaca_{'paper' if settings.alpaca_paper else 'live'}"
    return broker.value


async def _keepalive(router: ExecutionRouter, store: TradeStore) -> None:
    """Periodic tasks: equity snapshots, heartbeat logging."""
    while True:
        try:
            await asyncio.sleep(60)
            if router._account:
                acct = router._account
                broker_key = _broker_compound_key(acct.broker)
                await store.save_equity_snapshot(
                    broker=broker_key,
                    equity=acct.equity,
                    cash=acct.cash,
                    daily_pnl=acct.daily_pnl,
                    unreal_pnl=acct.unrealized_pnl,
                )
                state = router.state
                n_pos = len(state.positions)
                logger.info(
                    f"[heartbeat] equity=${acct.equity:,.2f} "
                    f"daily={acct.daily_pnl:+.2f} "
                    f"positions={n_pos} "
                    f"uptime={int(state.uptime_seconds)}s"
                )
        except asyncio.CancelledError: break
        except Exception as exc: logger.error(f"[keepalive] error: {exc}")

@app.command()
def main(
    symbols: str = typer.Option(
        None, "--symbols", "-s",
        help="Comma-separated instrument list (overrides .env)",
    ),
    broker: str = typer.Option(
        "alpaca", "--broker", "-b",
        help="Primary broker: alpaca | oanda | ninjatrader | ibkr | paper",
    ),
    tui: bool = typer.Option(
        False, "--tui", "-t",
        help="Launch Textual TUI dashboard",
    ),
    no_databento: bool = typer.Option(
        False, "--no-databento", "-D",
        help="Disable Databento feed",
    ),
    no_alpaca_feed: bool = typer.Option(
        False, "--no-alpaca-feed", "-A",
        help="Disable Alpaca data feed (broker still active)",
    ),
    schedule: str = typer.Option(
        None, "--schedule", "-S",
        help=(
            "Trading window: DAYS:STARTEND (UTC). "
            "DAYS = subset of 1234567 (1=Mon). "
            "E.g. '12345:09001700' = weekdays 09:00-17:00. "
            "Omit or '1234567:00000000' = always active."
        ),
    ),
    web: bool = typer.Option(
        False, "--web", "-w",
        help="Auto-start the Next.js dashboard (apps/web). Runs 'npm run dev' if no build exists, else 'npm start'.",
    ),
) -> None:
    """Start the KronosTrade automated trading system."""
    _configure_logging()

    # ── Optional: start Next.js dashboard as a subprocess ────────────────────
    _web_proc = None
    if web:
        import subprocess as _sp
        _web_root = Path(__file__).parents[1] / "apps" / "web"
        _has_build = (_web_root / ".next" / "BUILD_ID").exists()
        _cmd = ["npm", "start"] if _has_build else ["npm", "run", "dev"]
        try:
            _web_proc = _sp.Popen(_cmd, cwd=str(_web_root))
            logger.info(f"[web] dashboard started (pid={_web_proc.pid}) — http://localhost:3000")
        except FileNotFoundError:
            logger.warning("[web] npm not found — start the dashboard manually: cd apps/web && npm run dev")

    symbol_list = (
        [s.strip() for s in symbols.split(",") if s.strip()]
        if symbols
        else kats_cfg.instrument_list
    )

    broker_map = {
        "alpaca":       BrokerName.ALPACA,
        "oanda":        BrokerName.OANDA,
        "ninjatrader":  BrokerName.NINJATRADER,
        "ibkr":         BrokerName.IBKR,
        "paper":        BrokerName.PAPER,
    }
    broker_name = broker_map.get(broker.lower(), BrokerName.ALPACA)

    if tui:
        cwd = str(Path(__file__).parent.parent)
        sys.path.insert(0, str(Path(__file__).parent))
        from terminal_launcher import launch_terminals
        launch_terminals(cwd=cwd, use_demo=False)
        logger.info("[main] TUI + console windows launched")

    # Report any shell env vars that were auto-cleared at import time
    if _cleared_env:
        logger.warning(
            f"[config] Auto-cleared shell env overrides (using .env values instead):\n"
            + "\n".join(f"{k}" for k in _cleared_env.keys())
        )
    logger.info(
        f"[config] timeframe={kats_cfg.default_timeframe!r} "
        f"mode={kats_cfg.trading_mode.value} (from kats_config.json)"
    )
    # Log masked API keys so it's easy to confirm which account is active
    alpaca_key = settings.alpaca_api_key
    alpaca_masked = f"{alpaca_key[:4]}…{alpaca_key[-4:]}" if len(alpaca_key) >= 8 else "(not set)"
    logger.info(f"[config] alpaca key={alpaca_masked} paper={settings.alpaca_paper}")

    if settings.oanda_api_token:
        oanda_key = settings.oanda_api_token
        oanda_masked = f"{oanda_key[:4]}…{oanda_key[-4:]}" if len(oanda_key) >= 8 else "***"
        logger.info(
            f"[config] oanda key={oanda_masked} "
            f"account={settings.oanda_account_id} "
            f"practice={settings.oanda_practice}"
        )

    # Apply --schedule override (takes precedence over kats_config.json)
    if schedule:
        from kronos_trade.utils.schedule import TradingSchedule
        try:
            sched = TradingSchedule(schedule)
            kats_cfg.trading_schedule = schedule
            logger.info(f"[schedule] CLI override: {sched}")
        except ValueError as exc:
            logger.error(f"[schedule] invalid --schedule value: {exc}")
    elif kats_cfg.trading_schedule:
        from kronos_trade.utils.schedule import TradingSchedule
        try:
            sched = TradingSchedule(kats_cfg.trading_schedule)
            logger.info(f"[schedule] from config: {sched}")
        except ValueError as exc:
            logger.error(f"[schedule] invalid trading_schedule in config: {exc}")
            kats_cfg.trading_schedule = None
    else:
        logger.info("[schedule] no schedule — trading 24/7")

    try:
        asyncio.run(_run_async(
            symbols=symbol_list,
            broker_name=broker_name,
            enable_databento=not no_databento,
            enable_alpaca=not no_alpaca_feed,
        ))
    finally:
        if _web_proc is not None:
            _web_proc.terminate()
            logger.info("[web] dashboard stopped")


if __name__ == "__main__":
    app()
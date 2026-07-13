# KronosTrade

Automated trading system powered by [Kronos](https://github.com/shiyu-coder/Kronos) — the open-source financial foundation model trained on 12 billion K-line records across 45 global exchanges.

## Architecture

```
Market Data                Kronos Engine              Execution
────────────               ─────────────              ─────────
OANDA stream ──┐           Price forecast ─┐          OANDA (forex/CFD)
Alpaca WS ─────┼─► Pipeline ► Volatility  ─┼─► Risk ─► Alpaca (equities)
Databento ─────┘           Signal gen    ─┘  Gate    ► NinjaTrader 8

                    ┌──────────────────────────────────┐
                    │  Control Layer                   │
                    │  Textual TUI  (terminal)         │
                    │  FastAPI + WebSocket  :8765      │
                    │  Next.js Dashboard   :3000       │
                    └──────────────────────────────────┘
                                   │
                    ┌──────────────▼──────────────┐
                    │  SQLite Store               │
                    │  signals · orders · trades  │
                    │  equity snapshots           │
                    └─────────────────────────────┘
```

## Quick Start

```bash
# 1. Clone and set up Python environment
git clone https://github.com/robruon/kats.git
cd kronos-trade
chmod +x scripts/setup.sh && ./scripts/setup.sh

# 2. Fill in API keys
cp .env.example .env
nano .env

# 3. Start Redis
brew install redis && brew services start redis   # macOS
# or: docker run -d --name kronos-redis -p 6379:6379 redis:alpine

# 4. Install dashboard dependencies (first time only)
cd apps/web && npm install && cd ../..

# 5. Start the full system (engine + dashboard together)
python scripts/run_system.py --broker oanda --web

# Or start them separately:
python scripts/run_system.py --broker oanda   # Terminal 1
cd apps/web && npm run dev                    # Terminal 2

# 6. Open dashboard
open http://localhost:3000
```

## Project Structure

```
kronos-trade/
├── apps/
│   └── web/                       # Next.js 14 App Router dashboard
│       ├── app/
│       │   ├── page.tsx           # Live view (positions, signals, event log)
│       │   ├── journal/page.tsx   # Trade journal (stats, equity curve, history)
│       │   └── api/
│       │       ├── journal/       # trades · stats · equity (read SQLite directly)
│       │       └── engine/[...path]/ # Transparent proxy to FastAPI engine
│       ├── components/
│       │   ├── EngineContext.tsx  # Shared WS + broker state across all pages
│       │   ├── Header.tsx         # Nav + broker selector + equity display
│       │   ├── LiveView.tsx       # Signal log, open positions, recent signals
│       │   ├── JournalView.tsx    # Stats cards, equity chart, trade table
│       │   ├── EquityChart.tsx    # Recharts area chart
│       │   └── TradeTable.tsx     # Sortable, filterable closed-trade table
│       ├── hooks/
│       │   └── useWebSocket.ts    # Auto-reconnecting WS hook
│       └── .env.local             # DATABASE_PATH, engine URLs
├── kronos_trade/
│   ├── config.py                  # All settings via pydantic-settings
│   ├── models.py                  # Shared domain models
│   ├── data/
│   │   ├── feeds/
│   │   │   ├── alpaca_feed.py     # Alpaca WebSocket + REST history
│   │   │   ├── oanda_feed.py      # OANDA streaming price feed
│   │   │   └── databento_feed.py  # Databento futures/forex (optional)
│   │   └── pipeline.py            # Multi-feed aggregator + BarHistory
│   ├── kronos/
│   │   ├── predictor.py           # KronosEngine async wrapper
│   │   └── signals.py             # Forecast → KronosSignal
│   ├── strategy/
│   │   ├── engine.py              # Position sizing (fixed / volatility / kelly)
│   │   └── risk.py                # Risk gatekeeper (daily halt, drawdown, kill switch)
│   ├── execution/
│   │   ├── router.py              # Main trading loop
│   │   └── brokers/
│   │       ├── base.py            # Abstract broker adapter
│   │       ├── oanda.py           # OANDA v20 REST + transaction stream
│   │       ├── alpaca.py          # Alpaca WebSocket fills + REST
│   │       └── ninjatrader.py     # NT8 webhook bridge
│   ├── api/
│   │   └── main.py                # FastAPI REST + WebSocket server
│   ├── dashboard/
│   │   └── tui.py                 # Textual TUI control panel
│   ├── utils/
│   │   └── schedule.py            # Trading schedule parser (DAYS:STARTEND)
│   └── store/
│       └── db.py                  # SQLite async store (trades, signals, equity)
├── scripts/
│   ├── run_system.py              # Main entry point
│   ├── backtest.py                # Walk-forward Kronos backtest
│   ├── setup.sh                   # One-shot environment setup
│   └── nt8_webhook_bridge.cs      # NinjaScript companion for NT8
└── tests/
    ├── test_risk.py
    ├── test_strategy.py
    └── test_pipeline.py
```

## Configuration

Settings live in `.env` (secrets/infrastructure) and `kats_config.json` (trading params, auto-created on first run).

### .env — secrets and infrastructure

| Variable | Default | Description |
|---|---|---|
| `KRONOS_MODEL_SIZE` | `small` | `mini` / `small` / `base` |
| `KRONOS_DEVICE` | `cuda` | `cuda` / `cpu` / `mps` |
| `KRONOS_FORECAST_HORIZON` | `24` | Bars ahead to forecast |
| `KRONOS_MC_SAMPLES` | `50` | Monte Carlo samples for uncertainty bands |
| `DATABASE_URL` | `sqlite+aiosqlite:///./kronos_trade.db` | SQLite path |
| `API_PORT` | `8765` | FastAPI engine port |

### kats_config.json — trading parameters

Edited live via the TUI instruments panel, or directly in the file (takes effect on restart):

| Key | Default | Description |
|---|---|---|
| `default_timeframe` | `1h` | Bar timeframe |
| `trading_mode` | `paper` | `paper` / `live` |
| `min_signal_confidence` | `0.60` | Minimum directional probability to trade |
| `position_sizing` | `volatility` | `fixed` / `volatility` / `kelly` |
| `max_daily_loss_pct` | `2.0` | % of account — halts trading for the day |
| `max_drawdown_pct` | `5.0` | % from equity peak — halts |
| `default_rr_ratio` | `2.0` | Take-profit reward:risk ratio |
| `trading_schedule` | `null` | e.g. `"12345:08002200"` (see below) |

## Trading Schedule

Control when KATS places new trades using `DAYS:STARTEND` format (UTC):

```
"12345:08002200"          Weekdays 08:00–22:00 UTC
"12345:08002200,7:22002359"  Weekdays + Sunday evening
"1234567:00000000"        Always active (same as null)
```

- Days: `1`=Mon … `7`=Sun, any combination
- Times: 4-digit 24h UTC (`0800` = 08:00)
- Overnight windows supported: `"7:22000600"` = Sun 22:00 → Mon 06:00

Pass via CLI or set in `kats_config.json`:
```bash
python scripts/run_system.py --schedule "12345:08002200"
```

## Broker Setup

### OANDA (forex / CFD)

Recommended for forex. Supports native bracket orders (TP+SL in one submission), transaction streaming for real-time exit notifications, and closed-trade history sync.

1. Create account at [oanda.com](https://www.oanda.com) (practice or live)
2. Generate API token: My Account → Manage API Access
3. Add to `.env`:

```bash
OANDA_API_TOKEN=your_token_here
OANDA_ACCOUNT_ID=001-001-XXXXXXX-001
OANDA_PRACTICE=true    # false for live
```

4. Run:
```bash
python scripts/run_system.py --broker oanda --symbols EURUSD,GBPUSD,AUDJPY
```

**Forex market hours schedule** (OANDA is open Sun 17:00 ET → Fri 17:00 ET):
```bash
# In kats_config.json:
"trading_schedule": "12345:00002200,7:22002359"
```

### Alpaca (US equities / crypto)

Paper trading works out of the box. The system auto-filters instruments to only those your account can trade.

```bash
ALPACA_API_KEY=your_key
ALPACA_SECRET_KEY=your_secret
ALPACA_PAPER=true
```

```bash
python scripts/run_system.py --broker alpaca --symbols AAPL,TSLA,SPY
```

### NinjaTrader 8

1. Copy `scripts/nt8_webhook_bridge.cs` to your NT8 Custom Strategies folder
2. Compile in NT8 and add `WebhookBridge` strategy to a chart
3. Set in `.env`:

```bash
NT8_WEBHOOK_HOST=localhost
NT8_WEBHOOK_PORT=8080
NT8_ACCOUNT_ID=Sim101
```

```bash
python scripts/run_system.py --broker ninjatrader
```

## Dashboard

Two processes serve the dashboard:

| Process | Port | Purpose |
|---|---|---|
| FastAPI engine | `8765` | WebSocket event stream, REST control API |
| Next.js dashboard | `3000` | UI — Live view + Trade Journal |

**Start together:**
```bash
python scripts/run_system.py --web   # starts npm automatically
```

**Start separately** (recommended for development):
```bash
# Terminal 1
python scripts/run_system.py --broker oanda

# Terminal 2
cd apps/web && npm run dev
```

### Live View
- Real-time event log (signals, orders, fills, exits)
- Open positions with unrealized P&L, SL/TP levels
- Recent signals table with confidence bars
- Account equity and broker indicator in header

### Trade Journal
- Performance stats: win rate, profit factor, avg R:R, avg hold time
- Equity curve chart filtered by active broker account
- Per-symbol breakdown sidebar
- Full closed-trade history with sortable columns
- **Sync from broker**: pulls up to 90 days of closed trades from OANDA/Alpaca into the local DB — click `↓ Sync` in the filter bar

### Account / Broker Switching
The header dropdown and journal sidebar both show available brokers. Switching re-routes all new orders to the selected broker and re-filters the equity chart automatically.

## Engine API

FastAPI at `http://localhost:8765`:

| Endpoint | Method | Description |
|---|---|---|
| `/health` | GET | Liveness check |
| `/state` | GET | Full system state snapshot |
| `/positions` | GET | Open positions |
| `/account` | GET | Equity, cash, daily P&L |
| `/brokers` | GET | Available brokers + active |
| `/broker` | POST | `{"broker": "oanda"}` — switch active broker |
| `/signals/recent` | GET | Recent signals from DB (`?symbol=&limit=`) |
| `/sync-history` | POST | Pull closed trades from broker into DB (`?days_back=90`) |
| `/journal/trades` | GET | Closed trade records |
| `/journal/stats` | GET | Aggregate performance stats |
| `/journal/equity` | GET | Equity curve snapshots |
| `/kill-switch` | POST | `{"engage": true/false}` |
| `/close-all` | POST | Market-close all positions |
| `/ws` | WebSocket | Real-time event stream |

### WebSocket Event Types

All events follow `{"type": "...", "data": {...}}`:

| Type | Payload |
|---|---|
| `signal` | `{symbol, direction, confidence, entry_price, timeframe, ...}` |
| `order` | `{symbol, side, quantity, status, broker, ...}` |
| `positions` | `[{symbol, direction, entry_price, unrealized_pnl, ...}]` |
| `account` | `{equity, cash, daily_pnl, broker}` |
| `exit` | `{symbol, reason, price}` |
| `standby` | `{next_open, countdown}` — outside trading schedule |
| `broker_switch` | `{broker}` |
| `kill_switch` | `{active}` |

## Signal Logic

Kronos produces a probabilistic forecast (Monte Carlo samples) per bar:

1. **Directional bias** — fraction of MC samples predicting price higher than current at the horizon midpoint
2. `LONG` if `bullish_frac ≥ min_signal_confidence`
3. `SHORT` if `bearish_frac ≥ min_signal_confidence`
4. `None` (no trade) if below threshold

Position sizing uses the Kronos **volatility forecast** (uncertainty band spread) as an ATR proxy:

| Mode | Description |
|---|---|
| `volatility` | Risk budget ÷ ATR-based stop distance |
| `kelly` | Half-Kelly using Kronos confidence as win probability, capped at 10% |
| `fixed` | Constant dollar risk per trade |

## TUI Keybindings

| Key | Action |
|---|---|
| `K` | Toggle kill switch |
| `C` | Close all positions |
| `W` | Open web dashboard in browser |
| `P` | Pause / resume Kronos inference |
| `M` | Cycle trading mode (paper → live → paper) |
| `T` | Cycle timeframe |
| `Q` | Quit |

## Redis

Used for bar cache and system state persistence. Falls back gracefully if unavailable.

```bash
# macOS
brew install redis && brew services start redis

# Docker
docker run -d --name kronos-redis -p 6379:6379 redis:alpine

# Verify
redis-cli ping   # → PONG
```

## HuggingFace Token

Kronos model weights download from HuggingFace Hub on first run. Optional but avoids rate limits:

```bash
HF_TOKEN=hf_your_token_here
```

Weights are cached locally after the first download — token only needed once.

## Disclaimer

This software is for research and educational purposes. Automated trading involves substantial risk of loss. Always test thoroughly on paper accounts before risking real capital. Never trade with funds you cannot afford to lose.

#!/usr/bin/env python3
"""
scripts/seed_demo_db.py
Generate a demo SQLite database with realistic fake trade data.
Run from project root:
  poetry run python scripts/seed_demo_db.py
"""

import sqlite3
import random
import math
from datetime import datetime, timedelta, timezone

DB_PATH = "kronos_trade_demo.db"

random.seed(42)

SYMBOLS = ["BTCUSD", "ETHUSD", "EURUSD", "GBPUSD", "AUDJPY", "AAPL", "TSLA", "SPY"]
DIRECTIONS = ["long", "short"]
BROKER = "demo"

BASE_PRICES = {
    "BTCUSD": 67000, "ETHUSD": 3450, "EURUSD": 1.087,
    "GBPUSD": 1.270, "AUDJPY": 104.5, "AAPL": 224,
    "TSLA": 249, "SPY": 540,
}

def random_signal_confidence():
    return round(random.gauss(0.70, 0.10), 2)

def db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS trades (
        id INTEGER PRIMARY KEY, symbol TEXT, direction TEXT, quantity REAL,
        broker TEXT, timeframe TEXT, signal_confidence REAL,
        entry_price REAL, exit_price REAL, planned_sl REAL, planned_tp REAL,
        entry_datetime TEXT, exit_datetime TEXT, exit_reason TEXT,
        realized_pnl REAL, duration_seconds INTEGER, rr_achieved REAL, is_winner INTEGER
    )""")
    conn.execute("""
    CREATE TABLE IF NOT EXISTS equity_snapshots (
        id INTEGER PRIMARY KEY, timestamp TEXT, broker TEXT,
        equity REAL, cash REAL, daily_pnl REAL, unreal_pnl REAL
    )""")
    return conn

def seed_trades(conn):
    now = datetime.now(tz=timezone.utc)
    trades = []
    for i in range(120):
        sym = random.choice(SYMBOLS)
        base = BASE_PRICES[sym]
        direction = random.choice(DIRECTIONS)
        entry_price = round(base * random.uniform(0.98, 1.02), 2)
        is_winner = random.random() < 0.58
        rr = round(random.uniform(0.5, 4.0), 2) if is_winner else round(random.uniform(0.2, 1.8), 2)
        exit_price = round(
            entry_price * (1 + rr * 0.01) if direction == "long"
            else entry_price * (1 - rr * 0.01),
            2
        ) if is_winner else round(
            entry_price * (1 - 0.01) if direction == "long"
            else entry_price * (1 + 0.01),
            2
        )
        signal_conf = random_signal_confidence()
        quantity = round(random.uniform(0.01, 2.0), 4) if sym in ("BTCUSD", "ETHUSD") else random.randint(10, 10000)
        pnl = round((exit_price - entry_price) * quantity, 2) if direction == "long" else round(
            (entry_price - exit_price) * quantity, 2)
        sl = round(entry_price * (1 - 0.015), 2) if direction == "long" else round(entry_price * (1 + 0.015), 2)
        tp = round(entry_price * (1 + 0.025), 2) if direction == "long" else round(entry_price * (1 - 0.025), 2)
        hold_hours = random.uniform(0.5, 72)
        entry_dt = now - timedelta(hours=hold_hours + random.uniform(0, 48))
        exit_dt = entry_dt + timedelta(hours=hold_hours)
        exit_reason = random.choice(["tp", "sl", "manual"])
        duration = int(hold_hours * 3600)
        trades.append((
            i + 1, sym, direction, quantity, BROKER, "15m", signal_conf,
            entry_price, exit_price, sl, tp,
            entry_dt.isoformat(), exit_dt.isoformat(), exit_reason,
            pnl, duration, rr, 1 if is_winner else 0
        ))
    conn.executemany(
        "INSERT INTO trades VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        trades
    )
    conn.commit()
    print(f"  Inserted {len(trades)} trades")

def seed_equity(conn):
    now = datetime.now(tz=timezone.utc)
    equity = 100000.0
    snapshots = []
    for i in range(300):
        ts = now - timedelta(hours=i * 4)
        daily_pnl = random.gauss(150, 400)
        equity += daily_pnl
        snapshots.append((i + 1, ts.isoformat(), BROKER, round(equity, 2), round(equity * 0.95, 2), round(daily_pnl, 2), 0))
    conn.executemany(
        "INSERT INTO equity_snapshots VALUES (?,?,?,?,?,?,?)",
        snapshots
    )
    conn.commit()
    print(f"  Inserted {len(snapshots)} equity snapshots")
    print(f"  Equity range: $100,000 → ${equity:,.2f}")

def main():
    import os
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        print(f"  Removed existing {DB_PATH}")
    conn = db()
    seed_trades(conn)
    seed_equity(conn)
    conn.close()
    print(f"\n✅ Demo database created: {DB_PATH}")
    size = os.path.getsize(DB_PATH)
    print(f"   File size: {size / 1024:.1f} KB")

if __name__ == "__main__":
    main()
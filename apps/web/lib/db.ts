import Database from "better-sqlite3";
import path from "path";

const DB_PATH =
  process.env.DATABASE_PATH ??
  path.join(process.cwd(), "..", "..", "kronos_trade.db");

let _db: Database.Database | null = null;

export function getDb(): Database.Database {
  if (!_db) {
    try {
      _db = new Database(DB_PATH, { readonly: true, fileMustExist: true });
    } catch {
      // DB doesn't exist yet (engine not started) — return empty in-memory DB
      _db = new Database(":memory:");
      _db.exec(`
        CREATE TABLE IF NOT EXISTS trades (
          id INTEGER PRIMARY KEY, symbol TEXT, direction TEXT, quantity REAL,
          broker TEXT, timeframe TEXT, signal_confidence REAL,
          entry_price REAL, exit_price REAL, planned_sl REAL, planned_tp REAL,
          entry_datetime TEXT, exit_datetime TEXT, exit_reason TEXT,
          realized_pnl REAL, duration_seconds INTEGER, rr_achieved REAL, is_winner INTEGER
        );
        CREATE TABLE IF NOT EXISTS equity_snapshots (
          id INTEGER PRIMARY KEY, timestamp TEXT, broker TEXT,
          equity REAL, cash REAL, daily_pnl REAL, unreal_pnl REAL
        );
      `);
    }
  }
  return _db;
}

export function hasTable(table: string): boolean {
  const db = getDb();
  const row = db
    .prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`)
    .get(table) as { name: string } | undefined;
  return row != null;
}

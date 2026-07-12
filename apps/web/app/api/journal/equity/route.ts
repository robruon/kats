import { NextResponse } from "next/server";
import { getDb, hasTable } from "@/lib/db";

export const dynamic = "force-dynamic";

export async function GET(req: Request) {
  try {
    const { searchParams } = new URL(req.url);
    const limit  = Math.min(Number(searchParams.get("limit") ?? 500), 2000);
    const broker = searchParams.get("broker") ?? "";

    // Require a non-empty broker — never mix snapshots across accounts
    if (!broker) return NextResponse.json([]);

    const db = getDb();
    if (!hasTable("equity_snapshots")) return NextResponse.json([]);

    let rows = db
      .prepare(
        `SELECT timestamp, equity, daily_pnl
         FROM equity_snapshots
         WHERE broker = ?
         ORDER BY timestamp ASC
         LIMIT ?`
      )
      .all(broker, limit);

    // Fall back to plain broker name for data recorded before compound keys
    // were introduced (e.g. "oanda" instead of "oanda_live").  Once fresh
    // compound-key snapshots accumulate the fallback is never hit.
    if (rows.length === 0 && broker.includes("_")) {
      const plainBroker = broker.split("_")[0];
      rows = db
        .prepare(
          `SELECT timestamp, equity, daily_pnl
           FROM equity_snapshots
           WHERE broker = ?
           ORDER BY timestamp ASC
           LIMIT ?`
        )
        .all(plainBroker, limit);
    }

    return NextResponse.json(rows);
  } catch (err) {
    console.error("[journal/equity]", err);
    return NextResponse.json([]);
  }
}

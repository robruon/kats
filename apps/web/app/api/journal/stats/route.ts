import { NextResponse } from "next/server";
import { getDb, hasTable } from "@/lib/db";

export const dynamic = "force-dynamic";

export async function GET(req: Request) {
  try {
    const { searchParams } = new URL(req.url);
    const broker = searchParams.get("broker") ?? "";

    // Require broker — never mix stats across accounts
    if (!broker) {
      return NextResponse.json({
        total_trades: 0, winners: 0, losers: 0,
        win_rate: 0, total_pnl: 0,
        gross_profit: 0, gross_loss: 0, profit_factor: 0,
        avg_winner: 0, avg_loser: 0,
        best_trade: 0, worst_trade: 0,
        avg_duration_seconds: 0, avg_rr_achieved: 0,
        _no_broker: true,
      });
    }

    const db = getDb();
    if (!hasTable("trades")) {
      return NextResponse.json({ total_trades: 0, winners: 0, losers: 0, win_rate: 0, total_pnl: 0, gross_profit: 0, gross_loss: 0, profit_factor: 0, avg_winner: 0, avg_loser: 0, best_trade: 0, worst_trade: 0, avg_duration_seconds: 0, avg_rr_achieved: 0 });
    }

    const fetchTrades = (brokerVal: string) =>
      db
        .prepare(
          `SELECT realized_pnl, duration_seconds, rr_achieved
           FROM trades
           WHERE exit_datetime IS NOT NULL AND broker = ?`
        )
        .all(brokerVal) as { realized_pnl: number | null; duration_seconds: number | null; rr_achieved: number | null }[];

    let trades = fetchTrades(broker);

    // Fall back to plain broker name for trades recorded before compound keys
    if (trades.length === 0 && broker.includes("_")) {
      trades = fetchTrades(broker.split("_")[0]);
    }

    if (!trades.length) {
      return NextResponse.json({
        total_trades: 0, winners: 0, losers: 0,
        win_rate: 0, total_pnl: 0,
        gross_profit: 0, gross_loss: 0, profit_factor: 0,
        avg_winner: 0, avg_loser: 0,
        best_trade: 0, worst_trade: 0,
        avg_duration_seconds: 0, avg_rr_achieved: 0,
      });
    }

    const pnls    = trades.map((t) => t.realized_pnl ?? 0);
    const winners = pnls.filter((p) => p > 0);
    const losers  = pnls.filter((p) => p <= 0);
    const grossP  = winners.reduce((a, b) => a + b, 0);
    const grossL  = Math.abs(losers.reduce((a, b) => a + b, 0));
    const durs    = trades.map((t) => t.duration_seconds).filter(Boolean) as number[];
    const rrs     = trades.map((t) => t.rr_achieved).filter((r) => r != null) as number[];

    return NextResponse.json({
      total_trades:         trades.length,
      winners:              winners.length,
      losers:               losers.length,
      win_rate:             winners.length / trades.length,
      total_pnl:            pnls.reduce((a, b) => a + b, 0),
      gross_profit:         grossP,
      gross_loss:           grossL,
      profit_factor:        grossL > 0 ? grossP / grossL : 0,
      avg_winner:           winners.length ? grossP / winners.length : 0,
      avg_loser:            losers.length  ? losers.reduce((a, b) => a + b, 0) / losers.length : 0,
      best_trade:           pnls.length ? Math.max(...pnls) : 0,
      worst_trade:          pnls.length ? Math.min(...pnls) : 0,
      avg_duration_seconds: durs.length ? Math.round(durs.reduce((a, b) => a + b, 0) / durs.length) : 0,
      avg_rr_achieved:      rrs.length  ? rrs.reduce((a, b) => a + b, 0) / rrs.length : 0,
    });
  } catch (err) {
    console.error("[journal/stats]", err);
    return NextResponse.json({ error: "db error" }, { status: 500 });
  }
}

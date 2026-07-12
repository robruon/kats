import { NextResponse } from "next/server";
import { getDb, hasTable } from "@/lib/db";

export const dynamic = "force-dynamic";

export async function GET(req: Request) {
  try {
    const { searchParams } = new URL(req.url);
    const broker     = searchParams.get("broker") ?? "";
    const symbol     = searchParams.get("symbol");
    const direction  = searchParams.get("direction");
    const exitReason = searchParams.get("exit_reason");
    const closedOnly = searchParams.get("closed_only") !== "false";
    const limit      = Math.min(Number(searchParams.get("limit") ?? 200), 500);

    // Require broker — never mix trades across accounts
    if (!broker) return NextResponse.json([]);

    const db = getDb();
    if (!hasTable("trades")) return NextResponse.json([]);

    const buildQuery = (brokerVal: string) => {
      const conditions: string[] = ["broker = ?"];
      const params: (string | number)[] = [brokerVal];
      if (closedOnly) conditions.push("exit_datetime IS NOT NULL");
      if (symbol)     { conditions.push("symbol = ?");      params.push(symbol.toUpperCase()); }
      if (direction)  { conditions.push("direction = ?");   params.push(direction); }
      if (exitReason) { conditions.push("exit_reason = ?"); params.push(exitReason); }
      params.push(limit);
      return { sql: `SELECT * FROM trades WHERE ${conditions.join(" AND ")} ORDER BY entry_datetime DESC LIMIT ?`, params };
    };

    let { sql, params } = buildQuery(broker);
    let rows = db.prepare(sql).all(...params);

    // Fall back to plain broker name for trades recorded before compound keys
    if (rows.length === 0 && broker.includes("_")) {
      const plainBroker = broker.split("_")[0];
      ({ sql, params } = buildQuery(plainBroker));
      rows = db.prepare(sql).all(...params);
    }

    return NextResponse.json(rows);
  } catch (err) {
    console.error("[journal/trades]", err);
    return NextResponse.json({ error: "db error" }, { status: 500 });
  }
}

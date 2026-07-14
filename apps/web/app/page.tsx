"use client";

import { useState, useCallback, useEffect } from "react";
import { Header } from "@/components/Header";
import { StatsBar } from "@/components/StatsBar";
import { LiveView } from "@/components/LiveView";
import { useEngine, useEngineEvent } from "@/components/EngineContext";
import type { Position, Signal, WsEvent } from "@/lib/types";

const ENGINE  = process.env.NEXT_PUBLIC_ENGINE_URL ?? "/kats/api/engine";
const MAX_LOG = 200;
const MAX_SIG = 50;

function fmtUsd(n: number) {
  return (n >= 0 ? "+" : "") + n.toLocaleString("en-US", {
    style: "currency", currency: "USD", minimumFractionDigits: 2,
  });
}

export default function HomePage() {
  const { connected, equity, dailyPnl, standby, activeBroker } = useEngine();

  const [positions, setPositions] = useState<Position[]>([]);
  const [signals, setSignals]     = useState<Signal[]>([]);
  const [log, setLog]             = useState<string[]>([]);

  const pushLog = useCallback((line: string) => {
    const ts = new Date().toLocaleTimeString("en-US", { hour12: false });
    setLog((prev) => [`[${ts}] ${line}`, ...prev].slice(0, MAX_LOG));
  }, []);

  // ── Fetch recent signals from DB on mount ─────────────────────────────
  useEffect(() => {
    if (process.env.NEXT_PUBLIC_DEMO_MODE === "true") return;
    fetch(`${ENGINE}/signals/recent?limit=50`)
      .then((r) => r.ok ? r.json() : [])
      .then((rows: Signal[]) => {
        if (Array.isArray(rows) && rows.length) setSignals(rows);
      })
      .catch(() => {});
  }, []);

  // ── WebSocket events (live-only state: positions, log) ────────────────
  const onEvent = useCallback((evt: WsEvent) => {
    const data = evt.data as Record<string, unknown> | undefined;

    switch (evt.type) {
      case "signal": {
        const s = data as unknown as Signal;
        pushLog(`SIGNAL ${s.symbol} ${s.direction?.toUpperCase()} @ ${s.entry_price} conf=${((s.confidence ?? 0) * 100).toFixed(0)}%`);
        setSignals((prev) => [{ ...s, generated_at: s.generated_at ?? new Date().toISOString() }, ...prev].slice(0, MAX_SIG));
        break;
      }
      case "order": {
        const o = data as Record<string, unknown>;
        pushLog(`ORDER ${o.symbol} ${o.side} qty=${o.quantity} status=${o.status}`);
        break;
      }
      case "positions": {
        const pos = data as Position[] | undefined;
        if (Array.isArray(pos)) setPositions(pos);
        break;
      }
      case "risk_reject": {
        const r = data as { reason?: string } | undefined;
        pushLog(`RISK REJECT: ${r?.reason ?? "unknown"}`);
        break;
      }
      case "exit": {
        const x = data as { symbol?: string; reason?: string; price?: number } | undefined;
        pushLog(`EXIT ${x?.symbol} reason=${x?.reason} price=${x?.price}`);
        break;
      }
      case "kill_switch": {
        const k = data as { active?: boolean } | undefined;
        pushLog(`KILL SWITCH ${k?.active ? "ENGAGED 🔴" : "disengaged"}`);
        break;
      }
      case "error":
        pushLog(`ERROR ${evt.message ?? JSON.stringify(data)}`);
        break;
    }
  }, [pushLog]);

  // Subscribe to the shared WS in EngineContext — no second connection opened
  useEngineEvent(onEvent);

  const statsItems = [
    { label: "Open Positions", value: positions.length, muted: positions.length === 0 },
    {
      label: "Unrealized P&L",
      value: dailyPnl != null ? fmtUsd(dailyPnl) : "—",
      positive: dailyPnl != null ? dailyPnl > 0 ? true : dailyPnl < 0 ? false : undefined : undefined,
    },
    {
      label: "Equity",
      value: equity != null
        ? equity.toLocaleString("en-US", { style: "currency", currency: "USD", minimumFractionDigits: 2 })
        : "—",
      muted: true,
    },
    { label: "Account", value: activeBroker?.toUpperCase() || "—", muted: true },
    { label: "Signals", value: signals.length, muted: true },
    {
      label: "Status",
      value: !connected ? "OFFLINE" : standby ? "STANDBY" : "TRADING",
      positive: connected && !standby ? true : undefined,
      muted: !connected,
    },
  ];

  return (
    <div className="flex flex-col h-screen overflow-hidden">
      <Header />
      <StatsBar items={statsItems} />
      <div className="flex-1 min-h-0">
        <LiveView positions={positions} signals={signals} log={log} />
      </div>
    </div>
  );
}

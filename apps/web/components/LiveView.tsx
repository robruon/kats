"use client";

import clsx from "clsx";
import type { Position, Signal, WsEvent } from "@/lib/types";

interface LiveViewProps {
  positions: Position[];
  signals: Signal[];
  log: string[];
}

function fmt(n: number | undefined | null, decimals = 5) {
  if (n == null) return "—";
  return n.toLocaleString("en-US", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
}

function fmtPnl(n: number | undefined | null) {
  if (n == null) return "—";
  const prefix = n >= 0 ? "+" : "";
  return prefix + n.toFixed(2);
}

export function LiveView({ positions, signals, log }: LiveViewProps) {
  return (
    <div className="grid grid-cols-[1fr_340px] grid-rows-[1fr_180px] h-full overflow-hidden">
      {/* ── Signal log ── */}
      <div className="flex flex-col border-r border-border overflow-hidden">
        <div className="flex items-center justify-between px-4 py-2.5 border-b border-border shrink-0">
          <span className="text-[10px] uppercase tracking-widest text-ink-3">Signal Log</span>
        </div>
        <div className="flex-1 overflow-y-auto p-3 space-y-0.5">
          {log.length === 0 && (
            <p className="text-ink-3 text-[11px] p-2">Waiting for events…</p>
          )}
          {log.map((line, i) => (
            <div
              key={i}
              className="text-[11px] leading-5 text-ink-2 font-mono whitespace-pre-wrap break-all"
            >
              {line}
            </div>
          ))}
        </div>
      </div>

      {/* ── Sidebar: positions + signals ── */}
      <div className="flex flex-col overflow-hidden">
        {/* Open positions */}
        <div className="flex flex-col border-b border-border" style={{ minHeight: 0, flex: 1 }}>
          <div className="px-4 py-2.5 border-b border-border shrink-0">
            <span className="text-[10px] uppercase tracking-widest text-ink-3">
              Positions ({positions.length})
            </span>
          </div>
          <div className="flex-1 overflow-y-auto">
            {positions.length === 0 ? (
              <p className="text-ink-3 text-[11px] p-4">No open positions</p>
            ) : (
              positions.map((p) => (
                <div
                  key={p.symbol}
                  className={clsx(
                    "px-4 py-3 border-b border-border last:border-b-0 border-l-2",
                    p.direction === "long" ? "border-l-accent-green" : "border-l-accent-red"
                  )}
                >
                  <div className="flex items-center justify-between mb-1">
                    <span className="font-semibold text-[12px] text-ink">{p.symbol}</span>
                    <span
                      className={clsx(
                        "text-[11px] tabular-nums font-medium",
                        (p.unrealized_pnl ?? 0) >= 0 ? "text-accent-green" : "text-accent-red"
                      )}
                    >
                      {fmtPnl(p.unrealized_pnl)}
                    </span>
                  </div>
                  <div className="grid grid-cols-2 gap-x-2 text-[10px] text-ink-3">
                    <span>Entry {fmt(p.entry_price)}</span>
                    <span>Now {fmt(p.current_price)}</span>
                    <span>SL {p.stop_loss ? fmt(p.stop_loss) : "—"}</span>
                    <span>TP {p.take_profit ? fmt(p.take_profit) : "—"}</span>
                  </div>
                  <div className="mt-1 text-[9px] uppercase tracking-widest text-ink-3">
                    {p.direction} · {p.quantity} units · {p.broker}
                  </div>
                </div>
              ))
            )}
          </div>
        </div>
      </div>

      {/* ── Bottom: recent signals ── */}
      <div className="flex flex-col border-t border-border overflow-hidden col-span-2">
        <div className="px-4 py-2 border-b border-border shrink-0">
          <span className="text-[10px] uppercase tracking-widest text-ink-3">Recent Signals</span>
        </div>
        <div className="flex-1 overflow-x-auto overflow-y-auto">
          <table className="w-full text-[11px]">
            <thead className="sticky top-0 bg-surface-1">
              <tr className="text-[9px] uppercase tracking-widest text-ink-3">
                {["Time", "Symbol", "Direction", "Confidence", "Entry", "Timeframe"].map((h) => (
                  <th key={h} className="px-4 py-1.5 text-left font-medium whitespace-nowrap">
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {signals.map((s, i) => (
                <tr
                  key={i}
                  className="border-t border-border hover:bg-surface-2 transition-colors"
                >
                  <td className="px-4 py-2 text-ink-3 tabular-nums whitespace-nowrap">
                    {new Date(s.generated_at).toLocaleTimeString()}
                  </td>
                  <td className="px-4 py-2 font-semibold text-ink">{s.symbol}</td>
                  <td className="px-4 py-2">
                    <span
                      className={clsx(
                        "px-1.5 py-0.5 rounded text-[9px] uppercase font-semibold tracking-wider",
                        s.direction === "long" || s.direction === "buy"
                          ? "bg-green-900/40 text-accent-green"
                          : "bg-red-900/40 text-accent-red"
                      )}
                    >
                      {s.direction}
                    </span>
                  </td>
                  <td className="px-4 py-2 tabular-nums">
                    <div className="flex items-center gap-2">
                      <div className="w-16 h-1 bg-surface-3 rounded-full overflow-hidden">
                        <div
                          className="h-full bg-accent-amber rounded-full"
                          style={{ width: `${(s.confidence ?? 0) * 100}%` }}
                        />
                      </div>
                      <span className="text-ink-2">{((s.confidence ?? 0) * 100).toFixed(0)}%</span>
                    </div>
                  </td>
                  <td className="px-4 py-2 tabular-nums text-ink-2">{fmt(s.entry_price)}</td>
                  <td className="px-4 py-2 text-ink-3">{s.timeframe}</td>
                </tr>
              ))}
              {signals.length === 0 && (
                <tr>
                  <td colSpan={6} className="px-4 py-4 text-ink-3 text-center">
                    No signals yet
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

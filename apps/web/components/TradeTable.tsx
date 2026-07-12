"use client";

import { useState } from "react";
import clsx from "clsx";
import type { Trade } from "@/lib/types";

interface TradeTableProps {
  trades: Trade[];
}

function fmtDur(secs: number | null): string {
  if (!secs) return "—";
  if (secs < 3600) return `${Math.floor(secs / 60)}m`;
  if (secs < 86400) return `${(secs / 3600).toFixed(1)}h`;
  return `${(secs / 86400).toFixed(1)}d`;
}

function fmtPrice(n: number | null): string {
  if (n == null) return "—";
  return n.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 5 });
}

function fmtPnl(n: number | null): string {
  if (n == null) return "—";
  return (n >= 0 ? "+" : "") + n.toFixed(2);
}

const COLS = [
  "Time", "Symbol", "Dir", "Qty", "Entry", "Exit", "SL", "TP",
  "P&L", "RR", "Duration", "Reason", "Confidence"
];

export function TradeTable({ trades }: TradeTableProps) {
  const [sort, setSort] = useState<{ col: string; asc: boolean }>({ col: "Time", asc: false });

  const sorted = [...trades].sort((a, b) => {
    let av: number | string = 0, bv: number | string = 0;
    switch (sort.col) {
      case "Time":    av = a.entry_datetime; bv = b.entry_datetime; break;
      case "Symbol":  av = a.symbol;         bv = b.symbol;         break;
      case "P&L":     av = a.realized_pnl ?? -Infinity; bv = b.realized_pnl ?? -Infinity; break;
      case "RR":      av = a.rr_achieved ?? -Infinity;  bv = b.rr_achieved ?? -Infinity;  break;
      case "Duration":av = a.duration_seconds ?? 0; bv = b.duration_seconds ?? 0; break;
    }
    if (av < bv) return sort.asc ? -1 : 1;
    if (av > bv) return sort.asc ? 1 : -1;
    return 0;
  });

  function toggle(col: string) {
    setSort((s) => s.col === col ? { col, asc: !s.asc } : { col, asc: false });
  }

  return (
    <div className="overflow-auto h-full">
      <table className="w-full text-[11px] border-collapse">
        <thead className="sticky top-0 bg-surface-1 z-10">
          <tr>
            {COLS.map((col) => (
              <th
                key={col}
                onClick={() => toggle(col)}
                className="px-3 py-2 text-left text-[9px] uppercase tracking-widest text-ink-3 font-medium cursor-pointer hover:text-ink-2 whitespace-nowrap border-b border-border select-none"
              >
                {col}
                {sort.col === col && (
                  <span className="ml-1 opacity-60">{sort.asc ? "↑" : "↓"}</span>
                )}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {sorted.map((t) => {
            const rowClass = t.exit_datetime == null
              ? "table-row-open"
              : t.is_winner
              ? "table-row-win"
              : "table-row-loss";

            return (
              <tr
                key={t.id}
                className={clsx("border-b border-border hover:bg-surface-2 transition-colors", rowClass)}
              >
                <td className="px-3 py-2 tabular-nums text-ink-3 whitespace-nowrap">
                  {t.entry_datetime
                    ? new Date(t.entry_datetime).toLocaleString("en-US", {
                        month: "2-digit", day: "2-digit",
                        hour: "2-digit", minute: "2-digit",
                      })
                    : "—"}
                </td>
                <td className="px-3 py-2 font-semibold text-ink whitespace-nowrap">{t.symbol}</td>
                <td className="px-3 py-2 whitespace-nowrap">
                  <span
                    className={clsx(
                      "px-1.5 py-0.5 rounded text-[9px] uppercase font-semibold tracking-wider",
                      t.direction === "long"
                        ? "bg-green-900/40 text-accent-green"
                        : "bg-red-900/40 text-accent-red"
                    )}
                  >
                    {t.direction}
                  </span>
                </td>
                <td className="px-3 py-2 tabular-nums text-ink-2">{t.quantity}</td>
                <td className="px-3 py-2 tabular-nums text-ink-2">{fmtPrice(t.entry_price)}</td>
                <td className="px-3 py-2 tabular-nums text-ink-2">{fmtPrice(t.exit_price)}</td>
                <td className="px-3 py-2 tabular-nums text-ink-3">{fmtPrice(t.planned_sl)}</td>
                <td className="px-3 py-2 tabular-nums text-ink-3">{fmtPrice(t.planned_tp)}</td>
                <td className={clsx("px-3 py-2 tabular-nums font-semibold whitespace-nowrap",
                  (t.realized_pnl ?? 0) >= 0 ? "text-accent-green" : "text-accent-red"
                )}>
                  {fmtPnl(t.realized_pnl)}
                </td>
                <td className={clsx("px-3 py-2 tabular-nums",
                  (t.rr_achieved ?? 0) >= 1 ? "text-accent-green" : "text-ink-3"
                )}>
                  {t.rr_achieved != null ? `${t.rr_achieved.toFixed(2)}R` : "—"}
                </td>
                <td className="px-3 py-2 text-ink-3 whitespace-nowrap">{fmtDur(t.duration_seconds)}</td>
                <td className="px-3 py-2 text-ink-3 whitespace-nowrap">{t.exit_reason ?? "—"}</td>
                <td className="px-3 py-2 tabular-nums text-ink-3">
                  {t.signal_confidence != null
                    ? `${(t.signal_confidence * 100).toFixed(0)}%`
                    : "—"}
                </td>
              </tr>
            );
          })}
          {sorted.length === 0 && (
            <tr>
              <td colSpan={COLS.length} className="px-4 py-8 text-ink-3 text-center">
                No trades recorded yet
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}

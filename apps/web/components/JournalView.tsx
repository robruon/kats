"use client";

import { useState, useEffect, useCallback, useRef } from "react";
import { EquityChart } from "./EquityChart";
import { TradeTable } from "./TradeTable";
import { useEngine } from "./EngineContext";
import type { JournalStats, EquityPoint, Trade } from "@/lib/types";

function StatCard({ label, value, sub, positive }: {
  label: string; value: string; sub?: string; positive?: boolean;
}) {
  return (
    <div className="stat-card">
      <span className="stat-label">{label}</span>
      <span className={
        positive === true ? "stat-value text-accent-green"
        : positive === false ? "stat-value text-accent-red"
        : "stat-value"
      }>
        {value}
      </span>
      {sub && <span className="text-[10px] text-ink-3">{sub}</span>}
    </div>
  );
}

function fmtUsd(n: number) {
  return (n >= 0 ? "+" : "") + n.toLocaleString("en-US", { style: "currency", currency: "USD", minimumFractionDigits: 2 });
}

function fmtDur(secs: number): string {
  if (!secs) return "—";
  if (secs < 3600)  return `${Math.floor(secs / 60)}m`;
  if (secs < 86400) return `${(secs / 3600).toFixed(1)}h`;
  return `${(secs / 86400).toFixed(1)}d`;
}

interface Filters { symbol: string; direction: string; exit_reason: string; }

export function JournalView() {
  const { activeBroker, brokerKey, availableBrokers, brokerInfo, switchBroker, connected } = useEngine();

  const [stats, setStats]     = useState<JournalStats | null>(null);
  const [equity, setEquity]   = useState<EquityPoint[]>([]);
  const [trades, setTrades]   = useState<Trade[]>([]);
  const [filters, setFilters] = useState<Filters>({ symbol: "", direction: "", exit_reason: "" });
  const [loading, setLoading] = useState(true);
  const [syncing, setSyncing] = useState(false);
  const [syncMsg, setSyncMsg] = useState("");

  // Track which brokerKeys have been auto-synced this mount so we don't loop
  const autoSyncedRef = useRef(new Set<string>());

  const DEMO_MODE = typeof window !== "undefined" && process.env.NEXT_PUBLIC_DEMO_MODE === "true";

  const load = useCallback(async () => {
    // Don't load until we know which broker/account is active —
    // otherwise all three endpoints return mixed or empty data.
    // Use brokerKey (e.g. "oanda_live") so live and demo equity stay separate.
    if (!brokerKey) return;

    setLoading(true);
    try {
      const params = new URLSearchParams({ broker: brokerKey });
      if (filters.symbol)      params.set("symbol",      filters.symbol);
      if (filters.direction)   params.set("direction",   filters.direction);
      if (filters.exit_reason) params.set("exit_reason", filters.exit_reason);

      const equityParams = new URLSearchParams({ broker: brokerKey });

      const [s, e, t] = await Promise.all([
        fetch(`/api/journal/stats?broker=${brokerKey}`).then((r) => r.json()),
        fetch(`/api/journal/equity?${equityParams}`).then((r) => r.json()),
        fetch(`/api/journal/trades?${params}`).then((r) => r.json()),
      ]);
      setStats(s);
      setEquity(Array.isArray(e) ? e : []);
      setTrades(Array.isArray(t) ? t : []);
    } finally {
      setLoading(false);
    }
  }, [filters, brokerKey]);

  useEffect(() => { load(); }, [load]);

  const syncHistory = useCallback(async () => {
    setSyncing(true);
    setSyncMsg("");
    try {
      const res = await fetch("/api/engine/sync-history", { method: "POST" });
      const data = await res.json() as { synced?: number; error?: string };
      if (data.synced != null) {
        setSyncMsg(`Synced ${data.synced} trade${data.synced !== 1 ? "s" : ""} from broker`);
        if (data.synced > 0) await load();
      } else {
        setSyncMsg(data.error ?? "Sync failed");
      }
    } catch {
      setSyncMsg("Engine unreachable");
    } finally {
      setSyncing(false);
    }
  }, [load]);

  // Auto-pull broker history when the local DB has nothing for this account.
  // Fires once per brokerKey per mount; skips when filters are active (filtered
  // view may be empty even if there is local data).
  const noFilters = !filters.symbol && !filters.direction && !filters.exit_reason;
  useEffect(() => {
    if (
      !loading &&
      trades.length === 0 &&
      noFilters &&
      connected &&
      brokerKey &&
      !autoSyncedRef.current.has(brokerKey) &&
      !DEMO_MODE
    ) {
      autoSyncedRef.current.add(brokerKey);
      syncHistory();
    }
  }, [loading, trades.length, noFilters, connected, brokerKey, syncHistory, DEMO_MODE]);

  const symbols = Array.from(new Set(trades.map((t) => t.symbol))).sort();

  // Engine not yet contacted — broker key unknown, nothing to show yet
  if (!brokerKey) {
    return (
      <div className="flex items-center justify-center h-full text-ink-3 text-[11px] gap-2">
        <span className="w-1.5 h-1.5 rounded-full bg-accent-red" />
        Waiting for engine connection…
      </div>
    );
  }

  return (
    <div className="flex flex-col h-full overflow-hidden">
      {/* ── Stats row ── */}
      <div className="grid grid-cols-7 gap-3 p-4 shrink-0 border-b border-border bg-surface">
        {stats && stats.total_trades > 0 ? (
          <>
            <StatCard label="Total Trades"   value={String(stats.total_trades)} />
            <StatCard label="Win Rate"       value={`${(stats.win_rate * 100).toFixed(1)}%`}
              sub={`${stats.winners}W / ${stats.losers}L`} positive={stats.win_rate >= 0.5} />
            <StatCard label="Net P&L"        value={fmtUsd(stats.total_pnl)} positive={stats.total_pnl >= 0} />
            <StatCard label="Profit Factor"  value={stats.profit_factor > 0 ? stats.profit_factor.toFixed(2) : "—"}
              positive={stats.profit_factor >= 1} />
            <StatCard label="Avg Winner"     value={stats.avg_winner ? fmtUsd(stats.avg_winner) : "—"} positive={true} />
            <StatCard label="Avg Loser"      value={stats.avg_loser  ? fmtUsd(stats.avg_loser)  : "—"} positive={false} />
            <StatCard label="Avg RR"         value={stats.avg_rr_achieved ? `${stats.avg_rr_achieved.toFixed(2)}R` : "—"}
              sub={`Hold: ${fmtDur(stats.avg_duration_seconds)}`} positive={stats.avg_rr_achieved >= 1} />
          </>
        ) : (
          <div className="col-span-7 flex items-center justify-between">
            <span className="text-ink-3 text-[11px]">
              {loading  ? "Loading…"
              : syncing ? "Fetching history from broker…"
              : "No closed trades yet — sync from broker or wait for first exit"}
            </span>
            <button
              onClick={syncHistory}
              disabled={syncing || !connected}
              className="flex items-center gap-2 px-3 py-1.5 text-[10px] uppercase tracking-widest bg-accent-amber/10 border border-accent-amber/30 rounded text-accent-amber hover:bg-accent-amber/20 disabled:opacity-40 transition-colors"
            >
              {syncing ? "Syncing…" : "↓ Sync History"}
            </button>
          </div>
        )}
      </div>

      {/* ── Main content ── */}
      <div className="flex flex-1 min-h-0">
        <div className="flex flex-col flex-1 min-w-0">
          {/* Equity curve */}
          <div className="h-48 shrink-0 border-b border-border p-2">
            <EquityChart data={equity} />
          </div>

          {/* Filter bar */}
          <div className="flex items-center gap-3 px-4 py-2.5 border-b border-border shrink-0 bg-surface-1 flex-wrap">
            <span className="text-[9px] uppercase tracking-widest text-ink-3">Filter</span>

            <input
              type="text" placeholder="Symbol"
              value={filters.symbol}
              onChange={(e) => setFilters((f) => ({ ...f, symbol: e.target.value.toUpperCase() }))}
              className="w-24 bg-surface-2 border border-border rounded px-2 py-1 text-[11px] text-ink placeholder:text-ink-3 focus:outline-none focus:border-border-2"
            />
            <select value={filters.direction}
              onChange={(e) => setFilters((f) => ({ ...f, direction: e.target.value }))}
              className="bg-surface-2 border border-border rounded px-2 py-1 text-[11px] text-ink focus:outline-none focus:border-border-2"
            >
              <option value="">All directions</option>
              <option value="long">Long</option>
              <option value="short">Short</option>
            </select>
            <select value={filters.exit_reason}
              onChange={(e) => setFilters((f) => ({ ...f, exit_reason: e.target.value }))}
              className="bg-surface-2 border border-border rounded px-2 py-1 text-[11px] text-ink focus:outline-none focus:border-border-2"
            >
              <option value="">All exits</option>
              <option value="tp">Take Profit</option>
              <option value="sl">Stop Loss</option>
              <option value="manual">Manual</option>
            </select>

            <div className="flex items-center gap-2 ml-auto">
              {syncMsg && (
                <span className="text-[10px] text-ink-3">{syncMsg}</span>
              )}
              <button
                onClick={syncHistory}
                disabled={syncing || !connected}
                title={!connected ? "Engine offline" : "Pull closed trade history from broker"}
                className="px-3 py-1 text-[10px] uppercase tracking-widest bg-surface-2 border border-border rounded hover:border-border-2 text-ink-2 hover:text-ink disabled:opacity-40 transition-colors"
              >
                {syncing ? "Syncing…" : "↓ Sync"}
              </button>
              <button
                onClick={load}
                className="px-3 py-1 text-[10px] uppercase tracking-widest bg-surface-2 border border-border rounded hover:border-border-2 text-ink-2 hover:text-ink transition-colors"
              >
                Refresh
              </button>
              <span className="text-[10px] text-ink-3">{trades.length} trades</span>
            </div>
          </div>

          {/* Trade table */}
          <div className="flex-1 min-h-0">
            <TradeTable trades={trades} />
          </div>
        </div>

        {/* Symbol + account sidebar */}
        <div className="w-48 shrink-0 border-l border-border flex flex-col overflow-hidden">
          {/* Broker switcher */}
          {availableBrokers.length > 0 && (
            <div className="border-b border-border px-3 py-3 shrink-0">
              <p className="text-[9px] uppercase tracking-widest text-ink-3 mb-2">Account</p>
              <div className="space-y-1">
                {availableBrokers.map((b) => {
                  const env = brokerInfo[b]?.env ?? "";
                  const envLabel = env === "practice" ? "DEMO" : env === "live" ? "LIVE" : env.toUpperCase();
                  return (
                    <button
                      key={b}
                      onClick={() => switchBroker(b)}
                      className={`w-full text-left px-2 py-1.5 rounded text-[11px] uppercase tracking-wider transition-colors flex items-center justify-between ${
                        b === activeBroker
                          ? "bg-accent-amber/10 text-accent-amber border border-accent-amber/30"
                          : "text-ink-3 hover:text-ink hover:bg-surface-2"
                      }`}
                    >
                      <span>{b}</span>
                      <span className={`text-[8px] px-1 rounded ${
                        env === "live" ? "bg-red-900/40 text-accent-red"
                        : env === "practice" ? "bg-amber-900/30 text-accent-amber"
                        : "text-ink-3"
                      }`}>{envLabel}</span>
                    </button>
                  );
                })}
              </div>
            </div>
          )}

          {/* Symbol breakdown */}
          <div className="px-4 py-2.5 border-b border-border shrink-0">
            <span className="text-[9px] uppercase tracking-widest text-ink-3">By Symbol</span>
          </div>
          <div className="flex-1 overflow-y-auto">
            {symbols.length === 0 ? (
              <p className="text-ink-3 text-[10px] p-4">No data</p>
            ) : (
              symbols.map((sym) => {
                const st = trades.filter((t) => t.symbol === sym);
                const pnl = st.reduce((a, t) => a + (t.realized_pnl ?? 0), 0);
                const wins = st.filter((t) => t.is_winner).length;
                const wr = st.length ? wins / st.length : 0;
                return (
                  <button
                    key={sym}
                    onClick={() => setFilters((f) => ({ ...f, symbol: f.symbol === sym ? "" : sym }))}
                    className={`w-full text-left px-4 py-3 border-b border-border hover:bg-surface-2 transition-colors ${
                      filters.symbol === sym ? "bg-surface-2" : ""
                    }`}
                  >
                    <div className="flex items-center justify-between mb-0.5">
                      <span className="font-semibold text-[11px] text-ink">{sym}</span>
                      <span className={`text-[10px] tabular-nums ${pnl >= 0 ? "text-accent-green" : "text-accent-red"}`}>
                        {pnl >= 0 ? "+" : ""}{pnl.toFixed(0)}
                      </span>
                    </div>
                    <div className="text-[9px] text-ink-3">{st.length} trades · {(wr * 100).toFixed(0)}% win</div>
                    <div className="mt-1.5 h-0.5 bg-surface-3 rounded-full overflow-hidden">
                      <div className="h-full bg-accent-amber rounded-full" style={{ width: `${wr * 100}%` }} />
                    </div>
                  </button>
                );
              })
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

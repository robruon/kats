"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import clsx from "clsx";
import { useEngine } from "./EngineContext";

const NAV = [
  { href: "/", label: "Live" },
  { href: "/journal", label: "Journal" },
];

function fmtUsd(n: number) {
  return n.toLocaleString("en-US", { style: "currency", currency: "USD", minimumFractionDigits: 2 });
}

export function Header() {
  const path = usePathname();
  const { connected, equity, dailyPnl, standby, activeBroker, activeEnv, availableBrokers, brokerInfo, switchBroker } = useEngine();
  const pnlPositive = (dailyPnl ?? 0) >= 0;

  return (
    <header className="flex items-center bg-surface-1 border-b border-border h-12 shrink-0 px-5 gap-0">
      {/* Logo */}
      <span className="text-accent-amber font-bold tracking-widest uppercase text-sm mr-6 shrink-0">
        Kronos<span className="text-ink-3 font-light">Trade</span>
      </span>

      {/* Nav tabs */}
      <nav className="flex items-stretch h-full gap-1 mr-auto">
        {NAV.map((n) => {
          const active = path === n.href || (n.href !== "/" && path.startsWith(n.href));
          return (
            <Link
              key={n.href}
              href={n.href}
              className={clsx(
                "flex items-center px-4 text-[11px] uppercase tracking-widest border-b-2 transition-colors",
                active
                  ? "text-accent-amber border-accent-amber"
                  : "text-ink-3 border-transparent hover:text-ink-2 hover:border-border-2"
              )}
            >
              {n.label}
            </Link>
          );
        })}
      </nav>

      {/* Right side */}
      <div className="flex items-center gap-4 shrink-0">
        {standby && (
          <span className="text-[10px] text-amber-400 uppercase tracking-widest">
            ⏸ Standby · {standby}
          </span>
        )}

        {/* Broker selector */}
        {availableBrokers.length > 1 ? (
          <div className="flex items-center gap-1.5">
            <span className="text-[9px] uppercase tracking-widest text-ink-3">Acct</span>
            <select
              value={activeBroker}
              onChange={(e) => switchBroker(e.target.value)}
              className="bg-surface-2 border border-border rounded px-2 py-0.5 text-[11px] text-ink focus:outline-none focus:border-border-2 uppercase cursor-pointer"
            >
              {availableBrokers.map((b) => {
                const env = brokerInfo[b]?.env ?? "";
                const label = env === "live" ? `${b.toUpperCase()} LIVE`
                  : env === "practice" ? `${b.toUpperCase()} DEMO`
                  : b.toUpperCase();
                return <option key={b} value={b}>{label}</option>;
              })}
            </select>
          </div>
        ) : activeBroker ? (
          <div className="flex items-center gap-1.5">
            <span className="text-[9px] uppercase tracking-widest text-ink-3 border border-border rounded px-2 py-0.5">
              {activeBroker.toUpperCase()}
            </span>
            {activeEnv && (
              <span className={clsx(
                "text-[8px] uppercase tracking-widest rounded px-1.5 py-0.5 font-semibold",
                activeEnv === "live"     ? "bg-red-900/40 text-accent-red"
                : activeEnv === "practice" ? "bg-amber-900/30 text-accent-amber"
                : "text-ink-3 border border-border"
              )}>
                {activeEnv === "practice" ? "DEMO" : activeEnv.toUpperCase()}
              </span>
            )}
          </div>
        ) : null}

        {equity != null && (
          <span className="tabular-nums text-[11px] text-ink-2">{fmtUsd(equity)}</span>
        )}
        {dailyPnl != null && (
          <span className={clsx("tabular-nums text-[11px]", pnlPositive ? "text-accent-green" : "text-accent-red")}>
            {pnlPositive ? "+" : ""}{fmtUsd(dailyPnl)}
          </span>
        )}

        {/* Connection status */}
        <span className="flex items-center gap-1.5 text-[10px] uppercase tracking-widest text-ink-3">
          <span className={clsx(
            "w-1.5 h-1.5 rounded-full",
            connected ? "bg-accent-green animate-pulse2" : "bg-accent-red"
          )} />
          {connected ? "Live" : "Offline"}
        </span>
      </div>
    </header>
  );
}

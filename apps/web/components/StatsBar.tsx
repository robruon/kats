"use client";

import clsx from "clsx";

interface StatItem {
  label: string;
  value: string | number;
  positive?: boolean;
  muted?: boolean;
}

interface StatsBarProps {
  items: StatItem[];
}

export function StatsBar({ items }: StatsBarProps) {
  return (
    <div
      className="grid bg-surface-1 border-b border-border shrink-0"
      style={{ gridTemplateColumns: `repeat(${items.length}, 1fr)` }}
    >
      {items.map((item, i) => (
        <div
          key={i}
          className="flex flex-col justify-center px-4 py-2 border-r border-border last:border-r-0"
        >
          <span className="text-[9px] uppercase tracking-widest text-ink-3">{item.label}</span>
          <span
            className={clsx(
              "text-[13px] font-semibold tabular-nums leading-tight mt-0.5",
              item.positive === true && "text-accent-green",
              item.positive === false && "text-accent-red",
              item.muted && "text-ink-2",
              item.positive === undefined && !item.muted && "text-ink"
            )}
          >
            {item.value}
          </span>
        </div>
      ))}
    </div>
  );
}

"use client";

import {
  AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid,
} from "recharts";
import type { EquityPoint } from "@/lib/types";
import { format } from "date-fns";

interface EquityChartProps {
  data: EquityPoint[];
  color?: string;
}

const CustomTooltip = ({ active, payload, label }: any) => {
  if (!active || !payload?.length) return null;
  const equity = payload[0]?.value as number;
  const pnl = payload[1]?.value as number;
  return (
    <div className="bg-surface-1 border border-border rounded-lg px-3 py-2 text-[11px]">
      <p className="text-ink-3 mb-1">{label}</p>
      <p className="text-ink tabular-nums">Equity: ${equity?.toLocaleString("en-US", { minimumFractionDigits: 2 })}</p>
      {pnl != null && (
        <p className={pnl >= 0 ? "text-accent-green tabular-nums" : "text-accent-red tabular-nums"}>
          Daily P&L: {pnl >= 0 ? "+" : ""}{pnl.toFixed(2)}
        </p>
      )}
    </div>
  );
};

export function EquityChart({ data, color = "#a78bfa" }: EquityChartProps) {
  const formatted = data.map((d) => ({
    ...d,
    time: format(new Date(d.timestamp), "MM/dd HH:mm"),
  }));

  if (!data.length) {
    return (
      <div className="flex items-center justify-center h-full text-ink-3 text-[11px]">
        No equity data yet
      </div>
    );
  }

  const minEquity = Math.min(...data.map((d) => d.equity)) * 0.999;
  const maxEquity = Math.max(...data.map((d) => d.equity)) * 1.001;

  return (
    <ResponsiveContainer width="100%" height="100%">
      <AreaChart data={formatted} margin={{ top: 8, right: 16, left: 0, bottom: 0 }}>
        <defs>
          <linearGradient id="equityGrad" x1="0" y1="0" x2="0" y2="1">
            <stop offset="5%" stopColor={color} stopOpacity={0.3} />
            <stop offset="95%" stopColor={color} stopOpacity={0} />
          </linearGradient>
        </defs>
        <CartesianGrid strokeDasharray="3 3" stroke="#1a2332" vertical={false} />
        <XAxis
          dataKey="time"
          tick={{ fill: "#3a4f66", fontSize: 9 }}
          axisLine={false}
          tickLine={false}
          interval="preserveStartEnd"
        />
        <YAxis
          domain={[minEquity, maxEquity]}
          tick={{ fill: "#3a4f66", fontSize: 9 }}
          axisLine={false}
          tickLine={false}
          width={70}
          tickFormatter={(v) => `$${(v as number).toLocaleString("en-US", { minimumFractionDigits: 0 })}`}
        />
        <Tooltip content={<CustomTooltip />} />
        <Area
          type="monotone"
          dataKey="equity"
          stroke={color}
          strokeWidth={1.5}
          fill="url(#equityGrad)"
          dot={false}
          activeDot={{ r: 3, fill: color }}
        />
      </AreaChart>
    </ResponsiveContainer>
  );
}

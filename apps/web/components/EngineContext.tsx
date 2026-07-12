"use client";

/**
 * EngineContext — shared WebSocket + broker state across all pages.
 * Rendered once in the root layout; both Live and Journal consume it.
 */

import {
  createContext, useContext, useState, useEffect, useCallback,
  useRef, type ReactNode,
} from "react";
import type { WsEvent } from "@/lib/types";

type WsHandler = (evt: WsEvent) => void;

const ENGINE = process.env.NEXT_PUBLIC_ENGINE_URL ?? "http://localhost:8765";
const WS_URL  = process.env.NEXT_PUBLIC_WS_URL    ?? "ws://localhost:8765/ws";

export interface EngineState {
  connected:        boolean;
  activeBroker:     string;
  activeEnv:        string;   // "live" | "practice" | "paper"
  /** Compound key used for DB isolation: e.g. "oanda_live", "oanda_practice", "alpaca_paper" */
  brokerKey:        string;
  availableBrokers: string[];
  brokerInfo:       Record<string, { env: string }>;
  equity?:          number;
  dailyPnl?:        number;
  standby?:         string;
  switchBroker:     (broker: string) => Promise<void>;
  /**
   * Subscribe to raw WebSocket events from the shared connection.
   * Returns an unsubscribe function — call it from the useEffect cleanup.
   * All components share the single WS managed by EngineContext.
   */
  subscribe:        (handler: WsHandler) => () => void;
}

const Ctx = createContext<EngineState>({
  connected: false,
  activeBroker: "",
  activeEnv: "",
  brokerKey: "",
  availableBrokers: [],
  brokerInfo: {},
  switchBroker: async () => {},
  subscribe: () => () => {},
});

export function useEngine() { return useContext(Ctx); }

/**
 * Subscribe to raw WebSocket events through the shared EngineContext connection.
 * Avoids opening a second WebSocket from the same browser tab.
 *
 * @param handler  Called for every incoming WS event. Wrap in useCallback.
 */
export function useEngineEvent(handler: WsHandler) {
  const { subscribe } = useEngine();
  useEffect(() => subscribe(handler), [subscribe, handler]);
}

export function EngineProvider({ children }: { children: ReactNode }) {
  const [connected, setConnected]       = useState(false);
  const [activeBroker, setActiveBroker] = useState("");
  const [activeEnv, setActiveEnv]       = useState("");
  const [availBrokers, setAvailBrokers] = useState<string[]>([]);
  const [brokerInfo, setBrokerInfo]     = useState<Record<string, { env: string }>>({});
  const [equity, setEquity]             = useState<number>();
  const [dailyPnl, setDailyPnl]         = useState<number>();
  const [standby, setStandby]           = useState<string>();

  const wsRef        = useRef<WebSocket | null>(null);
  const reconnectRef = useRef<ReturnType<typeof setTimeout>>();
  const handlersRef  = useRef<Set<WsHandler>>(new Set());

  const subscribe = useCallback((handler: WsHandler) => {
    handlersRef.current.add(handler);
    return () => { handlersRef.current.delete(handler); };
  }, []);

  // ── Initial REST fetch ──────────────────────────────────────────────────
  useEffect(() => {
    async function fetchInitial() {
      try {
        const [br, ac] = await Promise.allSettled([
          fetch(`${ENGINE}/brokers`),
          fetch(`${ENGINE}/account`),
        ]);
        if (br.status === "fulfilled" && br.value.ok) {
          const b = await br.value.json() as {
            active: string; active_env: string;
            available: string[]; broker_info: Record<string, { env: string }>;
          };
          setActiveBroker(b.active ?? "");
          setActiveEnv(b.active_env ?? "");
          setAvailBrokers(b.available ?? [b.active]);
          setBrokerInfo(b.broker_info ?? {});
        }
        if (ac.status === "fulfilled" && ac.value.ok) {
          const a = await ac.value.json() as { equity?: number; daily_pnl?: number };
          if (a.equity   != null) setEquity(a.equity);
          if (a.daily_pnl != null) setDailyPnl(a.daily_pnl);
        }
      } catch {}
    }
    fetchInitial();
  }, []);

  // ── WebSocket ───────────────────────────────────────────────────────────
  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) return;
    const ws = new WebSocket(WS_URL);
    wsRef.current = ws;

    ws.onopen  = () => setConnected(true);
    ws.onclose = () => {
      setConnected(false);
      reconnectRef.current = setTimeout(connect, 3000);
    };
    ws.onerror = () => ws.close();

    ws.onmessage = (e) => {
      try {
        const evt = JSON.parse(e.data) as WsEvent;

        // Fan out to all subscribers (LiveView, etc.) before internal handling
        handlersRef.current.forEach((h) => h(evt));

        const data = evt.data as Record<string, unknown> | undefined;

        switch (evt.type) {
          case "account": {
            const a = data as { equity?: number; daily_pnl?: number } | undefined;
            if (a?.equity    != null) setEquity(a.equity);
            if (a?.daily_pnl != null) setDailyPnl(a.daily_pnl);
            break;
          }
          case "positions": {
            // equity may come from the positions event on some brokers
            if (typeof evt.equity === "number") setEquity(evt.equity);
            break;
          }
          case "equity_snapshot": {
            if (typeof evt.equity    === "number") setEquity(evt.equity);
            if (typeof evt.daily_pnl === "number") setDailyPnl(evt.daily_pnl);
            break;
          }
          case "broker_switch": {
            const b = data as { broker?: string; env?: string } | undefined;
            if (b?.broker) setActiveBroker(b.broker);
            if (b?.env)    setActiveEnv(b.env);
            break;
          }
          case "standby":
            setStandby((evt.next_open as string | undefined) || undefined);
            break;
        }
      } catch {}
    };
  }, []);

  useEffect(() => {
    connect();
    return () => {
      clearTimeout(reconnectRef.current);
      wsRef.current?.close();
    };
  }, [connect]);

  // ── Broker switch ───────────────────────────────────────────────────────
  const switchBroker = useCallback(async (broker: string) => {
    try {
      const res = await fetch(`${ENGINE}/broker`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ broker }),
      });
      if (res.ok) setActiveBroker(broker);
    } catch {}
  }, []);

  // Compound key mirrors what the Python backend stores in the DB:
  // "oanda_live", "oanda_practice", "alpaca_paper", "alpaca_live", etc.
  const brokerKey = activeBroker && activeEnv ? `${activeBroker}_${activeEnv}` : activeBroker;

  return (
    <Ctx.Provider value={{
      connected,
      activeBroker,
      activeEnv,
      brokerKey,
      availableBrokers: availBrokers,
      brokerInfo,
      equity,
      dailyPnl,
      standby,
      switchBroker,
      subscribe,
    }}>
      {children}
    </Ctx.Provider>
  );
}

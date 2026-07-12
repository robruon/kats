"""
kronos_trade/api/main.py
FastAPI REST + WebSocket server.
WebSocket at /ws streams all system events to the web dashboard.
"""
from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from loguru import logger
from pydantic import BaseModel

# Router is injected at startup via app.state
# (avoids circular imports — router is constructed in run_system.py)

# ── WebSocket connection manager ──────────────────────────────────────────────

class ConnectionManager:
    def __init__(self) -> None:
        self._active: list[WebSocket] = []

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        self._active.append(ws)
        logger.info(f"[ws] client connected | total={len(self._active)}")

    def disconnect(self, ws: WebSocket) -> None:
        self._active.remove(ws)
        logger.info(f"[ws] client disconnected | total={len(self._active)}")

    async def broadcast(self, data: dict) -> None:
        dead = []
        for ws in self._active:
            try:
                await ws.send_text(json.dumps(data))
            except Exception:
                dead.append(ws)
        for ws in dead:
            self._active.remove(ws)


ws_manager = ConnectionManager()


# ── App factory ───────────────────────────────────────────────────────────────

def create_app(router_instance=None, store_instance=None) -> FastAPI:

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Start the broadcast pump if router is attached
        if router_instance:
            app.state.router = router_instance
            bq = router_instance.subscribe_broadcasts()
            app.state.broadcast_task = asyncio.create_task(
                _pump_broadcasts(bq), name="ws-pump"
        )
        if store_instance:
            app.state.store = store_instance
        yield
        await ws_manager.broadcast({"type": "shutdown", "data": {"reason", "system_stopped"}})
        await asyncio.sleep(0.3)
        if hasattr(app.state, "broadcast_task"):
            app.state.broadcast_task.cancel()

    app = FastAPI(
        title="KronosTrade API",
        version="0.1.0",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ── REST routes ───────────────────────────────────────────────────────────

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    @app.get("/state")
    async def get_state():
        rt = getattr(app.state, "router", None)
        if not rt:
            raise HTTPException(503, "Router not initialized")
        return rt.state.model_dump(mode="json")

    @app.get("/positions")
    async def get_positions():
        rt = getattr(app.state, "router", None)
        if not rt:
            raise HTTPException(503, "Router not initialized")
        return [p.model_dump(mode="json") for p in rt._positions]

    @app.get("/account")
    async def get_account():
        rt = getattr(app.state, "router", None)
        if not rt:
            raise HTTPException(503, "Router not initialized")
        if not rt._account:
            raise HTTPException(404, "No account data yet")
        return rt._account.model_dump(mode="json")

    @app.get("/brokers")
    async def get_brokers():
        rt = getattr(app.state, "router", None)
        if not rt:
            raise HTTPException(503, "Router not initialized")

        from kronos_trade.config import settings as _s

        def _broker_env(name: str) -> str:
            """Return 'live', 'practice', or 'paper' for a broker name."""
            if name == "oanda":
                return "practice" if _s.oanda_practice else "live"
            if name == "alpaca":
                return "paper" if _s.alpaca_paper else "live"
            return "paper"

        available = rt.available_brokers  # list[str]
        return {
            "active":      rt.primary_broker.value,
            "active_env":  _broker_env(rt.primary_broker.value),
            "available":   available,
            "broker_info": {
                name: {"env": _broker_env(name)}
                for name in available
            },
        }

    # ── Instruments ───────────────────────────────────────────────────────────

    @app.get("/instruments/available")
    async def get_available_instruments():
        rt = getattr(app.state, "router", None)
        if not rt:
            raise HTTPException(503, "Router not initialized")
        from kronos_trade.models import asset_class as get_ac
        broker = rt.brokers.get(rt.primary_broker)
        syms   = sorted(broker.supported_symbols) if broker else []
        return {
            "symbols": [
                {"symbol": s, "asset_class": (get_ac(s) or "unknown")}
                for s in syms
            ],
            "active": rt.symbols,
        }

    @app.get("/instruments/active")
    async def get_active_instruments():
        rt = getattr(app.state, "router", None)
        if not rt:
            raise HTTPException(503, "Router not initialized")
        return {"instruments": rt.symbols}

    class InstrumentsRequest(BaseModel):
        instruments: list[str]

    @app.post("/instruments")
    async def set_instruments(req: InstrumentsRequest):
        rt = getattr(app.state, "router", None)
        if not rt:
            raise HTTPException(503, "Router not initialized")
        await rt.update_instruments(req.instruments)
        return {"instruments": req.instruments}

    # ── Broker ────────────────────────────────────────────────────────────────

    class BrokerRequest(BaseModel):
        broker: str

    @app.post("/broker")
    async def switch_broker(req: BrokerRequest):
        rt = getattr(app.state, "router", None)
        if not rt:
            raise HTTPException(503, "Router not initialized")
        from kronos_trade.models import BrokerName
        try:
            name = BrokerName(req.broker)
        except ValueError:
            raise HTTPException(400, f"Unknown broker '{req.broker}'")
        try:
            await rt.switch_broker(name)
        except ValueError as exc:
            raise HTTPException(400, str(exc))
        return {"active": req.broker}

    @app.post("/kronos/pause")
    async def kronos_pause():
        rt = getattr(app.state, "router", None)
        if not rt:
            raise HTTPException(503, "Router not initialized")
        await rt.pause_kronos()
        return {"paused": True}

    @app.post("/kronos/resume")
    async def kronos_resume():
        rt = getattr(app.state, "router", None)
        if not rt:
            raise HTTPException(503, "Router not initialized")
        await rt.resume_kronos()
        return {"paused": False}

    @app.get("/kronos/status")
    async def kronos_status():
        rt = getattr(app.state, "router", None)
        if not rt:
            raise HTTPException(503, "Router not initialized")
        return {"paused": rt.kronos_paused, "loaded": rt.kronos.is_loaded}

    @app.get("/kronos/settings")
    async def get_kronos_settings():
        from kronos_trade.config import kats_cfg
        return {
            "model_size":       kats_cfg.kronos_model_size,
            "device":           kats_cfg.kronos_device,
            "max_context":      kats_cfg.kronos_max_context,
            "forecast_horizon": kats_cfg.kronos_forecast_horizon,
            "mc_samples":       kats_cfg.kronos_mc_samples,
        }

    class KronosSettingsRequest(BaseModel):
        model_size:       str | None = None
        device:           str | None = None
        max_context:      int | None = None
        forecast_horizon: int | None = None
        mc_samples:       int | None = None

    @app.post("/kronos/settings")
    async def set_kronos_settings(req: KronosSettingsRequest):
        rt = getattr(app.state, "router", None)
        if not rt:
            raise HTTPException(503, "Router not initialized")
        cfg = {k: v for k, v in req.model_dump().items() if v is not None}
        if not cfg:
            raise HTTPException(400, "No settings provided")
        if "model_size" in cfg and cfg["model_size"] not in {"mini", "small", "base"}:
            raise HTTPException(400, "model_size must be one of: mini, small, base")
        if "device" in cfg and cfg["device"] not in {"cpu", "mps", "cuda"}:
            raise HTTPException(400, "device must be one of: cpu, mps, cuda")
        await rt.update_kronos_settings(cfg)
        return {"status": "restarting", "settings": cfg}

    @app.get("/mode")
    async def get_mode():
        rt = getattr(app.state, "router", None)
        if not rt:
            raise HTTPException(503, "Router not initialized")
        return {"mode": rt.trading_mode.value}

    class ModeRequest(BaseModel):
        mode: str

    @app.post("/mode")
    async def set_mode(req: ModeRequest):
        rt = getattr(app.state, "router", None)
        if not rt:
            raise HTTPException(503, "Router not initialized")
        from kronos_trade.config import TradingMode
        try:
            mode = TradingMode(req.mode.lower())
        except ValueError:
            raise HTTPException(400, f"Unknown mode '{req.mode}'; valid: scalping, day, swing")
        await rt.switch_trading_mode(mode)
        return {"mode": mode.value}

    _VALID_TIMEFRAMES = ["1m", "5m", "15m", "1h", "4h", "1d"]

    @app.get("/timeframe")
    async def get_timeframe():
        rt = getattr(app.state, "router", None)
        if not rt:
            raise HTTPException(503, "Router not initialized")
        return {"timeframe": rt.timeframe}

    class TimeframeRequest(BaseModel):
        timeframe: str

    @app.post("/timeframe")
    async def set_timeframe(req: TimeframeRequest):
        rt = getattr(app.state, "router", None)
        if not rt:
            raise HTTPException(503, "Router not initialized")
        if req.timeframe not in _VALID_TIMEFRAMES:
            raise HTTPException(
                400,
                f"Invalid timeframe '{req.timeframe}'; valid: {', '.join(_VALID_TIMEFRAMES)}"
            )
        await rt.switch_timeframe(req.timeframe)
        return {"timeframe": req.timeframe}

    # ── Strategy tuning ───────────────────────────────────────────────────────

    class StrategyRequest(BaseModel):
        min_signal_confidence: float | None = None
        default_rr_ratio:      float | None = None
        sl_mult_override:      float | None = None   # None = use mode default
        tp_mult_override:      float | None = None   # None = use mode default
        position_sizing:       str   | None = None

    @app.get("/strategy")
    async def get_strategy():
        from kronos_trade.config import kats_cfg
        return {
            "min_signal_confidence": kats_cfg.min_signal_confidence,
            "default_rr_ratio":      kats_cfg.default_rr_ratio,
            "sl_mult_override":      kats_cfg.sl_mult_override,
            "tp_mult_override":      kats_cfg.tp_mult_override,
            "position_sizing":       kats_cfg.position_sizing,
        }

    @app.post("/strategy")
    async def set_strategy(req: StrategyRequest):
        from kronos_trade.config import kats_cfg
        changed = False
        if req.min_signal_confidence is not None:
            kats_cfg.min_signal_confidence = req.min_signal_confidence
            changed = True
        if req.default_rr_ratio is not None:
            kats_cfg.default_rr_ratio = req.default_rr_ratio
            changed = True
        if req.position_sizing is not None:
            kats_cfg.position_sizing = req.position_sizing
            changed = True
        # Allow explicit None to clear overrides (reset to mode defaults)
        if "sl_mult_override" in req.model_fields_set:
            kats_cfg.sl_mult_override = req.sl_mult_override
            changed = True
        if "tp_mult_override" in req.model_fields_set:
            kats_cfg.tp_mult_override = req.tp_mult_override
            changed = True
        if changed:
            kats_cfg.save()
            # Also refresh the in-memory strategy engine on the router
            rt = getattr(app.state, "router", None)
            if rt:
                rt.strategy.sizing_mode = kats_cfg.position_sizing
                rt.strategy.rr_ratio    = kats_cfg.default_rr_ratio
        return {
            "min_signal_confidence": kats_cfg.min_signal_confidence,
            "default_rr_ratio":      kats_cfg.default_rr_ratio,
            "sl_mult_override":      kats_cfg.sl_mult_override,
            "tp_mult_override":      kats_cfg.tp_mult_override,
            "position_sizing":       kats_cfg.position_sizing,
        }

    class KillSwitchRequest(BaseModel):
        engage: bool

    @app.post("/kill-switch")
    async def kill_switch(req: KillSwitchRequest):
        rt = getattr(app.state, "router", None)
        if not rt:
            raise HTTPException(503, "Router not initialized")
        if req.engage:
            await rt.engage_kill_switch()
        else:
            await rt.disengage_kill_switch()
        return {"kill_switch": req.engage}

    class CloseRequest(BaseModel):
        symbol: str

    @app.post("/close-position")
    async def close_position(req: CloseRequest):
        rt = getattr(app.state, "router", None)
        if not rt: raise HTTPException(503, "Router not initialized")
        broker = rt.brokers.get(rt.primary_broker)
        if not broker: raise HTTPException(503, "No broker")
        ok = await broker.close_position(req.symbol)
        if not ok:
            raise HTTPException(500, f"Broker failed to close {req.symbol}")
        rt._position_params.pop(req.symbol, None)
        rt._submitted_at.pop(req.symbol, None)
        await rt._refresh_positions()
        return {"closed": req.symbol}

    @app.get("/bars/{symbol}")
    async def get_bars(symbol: str, n: int = 100):
        rt = getattr(app.state, "router", None)
        if not rt: raise HTTPException(503, "Router not initialized")
        history = rt.pipeline.history(symbol)
        if not history: return []
        df, ts = history.to_kronos_df()
        bars = []
        for i in range(len(df)):
            row = df.iloc[i]
            t    = ts.iloc[i]
            try: unix = int(t.timestamp())
            except Exception: continue
            bars.append({
                "time":  unix,
                "open":  float(row["open"]),
                "high":  float(row["high"]),
                "low":   float(row["low"]),
                "close": float(row["close"]),
            })
        return bars[-n:]

    @app.post("/close-winning")
    async def close_winning():
        rt = getattr(app.state, "router", None)
        if not rt:
            raise HTTPException(503, "Router not initialized")
        broker  = rt.brokers.get(rt.primary_broker)
        closed  = []
        for pos in list(rt._positions):
            if pos.unrealized_pnl > 0:
                ok = await broker.close_position(pos.symbol)
                if ok:
                    rt._position_params.pop(pos.symbol, None)
                    rt._submitted_at.pop(pos.symbol, None)
                    closed.append(pos.symbol)
        if closed:
            await rt._refresh_positions()
        return {"closed": closed}

    @app.post("/close-losing")
    async def close_losing():
        rt = getattr(app.state, "router", None)
        if not rt:
            raise HTTPException(503, "Router not initialized")
        broker = rt.brokers.get(rt.primary_broker)
        closed = []
        for pos in list(rt._positions):
            if pos.unrealized_pnl < 0:
                ok = await broker.close_position(pos.symbol)
                if ok:
                    rt._position_params.pop(pos.symbol, None)
                    rt._submitted_at.pop(pos.symbol, None)
                    closed.append(pos.symbol)
        if closed:
            await rt._refresh_positions()
        return {"closed": closed}

    @app.post("/close-all")
    async def close_all():
        rt = getattr(app.state, "router", None)
        if not rt:
            raise HTTPException(503, "Router not initialized")
        await rt.close_all_positions()
        return {"closed": True}

    # ── WebSocket ─────────────────────────────────────────────────────────────

    @app.websocket("/ws")
    async def websocket_endpoint(ws: WebSocket):
        await ws_manager.connect(ws)
        try:
            while True:
                await ws.receive_text()   # keep alive (client can send pings)
        except WebSocketDisconnect:
            ws_manager.disconnect(ws)

    @app.get("/dashboard", include_in_schema=False)
    async def serve_dashboard():
        return RedirectResponse("http://localhost:3000")

    # ── Journal endpoints ─────────────────────────────────────────────────────

    @app.get("/journal/trades")
    async def journal_trades(
        limit: int = 200,
        symbol: str | None = None,
        open: bool = False,
    ):
        st = getattr(app.state, "store", None)
        if not st:
            return []
        return await st.journal_trades(
            limit=limit, symbol=symbol, closed_only=not open
        )

    @app.get("/journal/stats")
    async def journal_stats():
        st = getattr(app.state, "store", None)
        if not st:
            return {}
        return await st.journal_stats()

    @app.get("/journal/equity")
    async def journal_equity(limit: int = 500):
        st = getattr(app.state, "store", None)
        if not st:
            return []
        return await st.equity_curve(limit=limit)

    # ── Signals ───────────────────────────────────────────────────────────────

    @app.get("/signals/recent")
    async def signals_recent(symbol: str | None = None, limit: int = 50):
        st = getattr(app.state, "store", None)
        if not st:
            return []
        return await st.recent_signals(symbol=symbol, limit=limit)

    # ── Broker history sync ───────────────────────────────────────────────────

    @app.post("/sync-history")
    async def sync_history(days_back: int = 90):
        rt = getattr(app.state, "router", None)
        st = getattr(app.state, "store", None)
        if not rt or not st:
            raise HTTPException(503, "Engine not ready")
        broker = rt.brokers.get(rt.primary_broker)
        if broker is None:
            raise HTTPException(503, "No active broker")
        sync_fn = getattr(broker, "sync_history", None)
        if sync_fn is None:
            raise HTTPException(400, f"Broker {rt.primary_broker.value} does not support history sync")
        try:
            count = await sync_fn(st, days_back=days_back)
            return {"synced": count, "broker": rt.primary_broker.value}
        except Exception as exc:
            logger.error(f"[api] sync_history error: {exc}")
            raise HTTPException(500, str(exc))

    return app


async def _pump_broadcasts(queue: asyncio.Queue) -> None:
    """Forward router events to all WebSocket clients."""
    while True:
        try:
            event = await queue.get()
            await ws_manager.broadcast(event)
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.error(f"[ws-pump] error: {exc}")

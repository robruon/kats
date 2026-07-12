"""
kronos_trade/store/db.py
Async SQLite store — trade log, signal history, daily P&L snapshots.
Uses SQLAlchemy 2.0 async engine + aiosqlite.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

from loguru import logger
from sqlalchemy import (
    JSON, Boolean, Column, Date, DateTime, Float, Integer, String,
    Text, delete, func, select, update,
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from kronos_trade.config import settings
from kronos_trade.models import BracketOrder, KronosSignal, OrderStatus


# ── ORM models ────────────────────────────────────────────────────────────────

class Base(DeclarativeBase):
    pass


class SignalRecord(Base):
    __tablename__ = "signals"

    id               = Column(Integer, primary_key=True, autoincrement=True)
    symbol           = Column(String(20), nullable=False, index=True)
    generated_at     = Column(DateTime, nullable=False, index=True)
    timeframe        = Column(String(10))
    direction        = Column(String(10))
    confidence       = Column(Float)
    entry_price      = Column(Float)
    atr              = Column(Float)
    volatility       = Column(Float)
    forecast_mean    = Column(JSON)   # list[float]
    forecast_lower   = Column(JSON)
    forecast_upper   = Column(JSON)


class OrderRecord(Base):
    __tablename__ = "orders"

    id              = Column(Integer, primary_key=True, autoincrement=True)
    signal_id       = Column(String(100), index=True)
    symbol          = Column(String(20), nullable=False, index=True)
    side            = Column(String(10))
    quantity        = Column(Float)
    entry_price     = Column(Float)
    stop_loss       = Column(Float)
    take_profit     = Column(Float)
    broker          = Column(String(30))
    status          = Column(String(20), default="pending")
    broker_order_id = Column(String(100))
    filled_price    = Column(Float, nullable=True)
    created_at      = Column(DateTime, default=lambda: datetime.now(tz=timezone.utc))
    filled_at       = Column(DateTime, nullable=True)
    realized_pnl    = Column(Float, nullable=True)


class TradeRecord(Base):
    """
    One completed trade: entry fill → exit.
    Created when a bracket order fills; updated when the position closes.
    """
    __tablename__ = "trades"

    id                 = Column(Integer, primary_key=True, autoincrement=True)
    broker_order_id    = Column(String(100), index=True)   # OANDA tradeID / Alpaca order ID
    symbol             = Column(String(20), nullable=False, index=True)
    direction          = Column(String(10))                 # long / short
    quantity           = Column(Float)
    broker             = Column(String(30))
    timeframe          = Column(String(10))
    signal_confidence  = Column(Float, nullable=True)

    # Entry
    entry_price        = Column(Float)
    planned_sl         = Column(Float)
    planned_tp         = Column(Float)
    entry_datetime     = Column(DateTime, nullable=False, index=True)

    # Exit (populated on close)
    exit_price         = Column(Float,    nullable=True)
    exit_datetime      = Column(DateTime, nullable=True)
    exit_reason        = Column(String(30), nullable=True)  # tp / sl / manual / unknown

    # Outcome
    realized_pnl       = Column(Float, nullable=True)
    duration_seconds   = Column(Integer, nullable=True)
    r_risked           = Column(Float, nullable=True)       # |entry - sl|
    rr_achieved        = Column(Float, nullable=True)       # realized_pnl / (r_risked × qty)
    is_winner          = Column(Boolean, nullable=True)


class EquitySnapshot(Base):
    __tablename__ = "equity_snapshots"

    id         = Column(Integer, primary_key=True, autoincrement=True)
    timestamp  = Column(DateTime, nullable=False, index=True)
    broker     = Column(String(30))
    equity     = Column(Float)
    cash       = Column(Float)
    daily_pnl  = Column(Float)
    unreal_pnl = Column(Float)


class SchemaVersion(Base):
    """Single-row table used to track one-time schema migrations."""
    __tablename__ = "schema_version"

    version = Column(Integer, primary_key=True)


# ── Store class ───────────────────────────────────────────────────────────────

class TradeStore:
    """Thin async wrapper — call .init() before using."""

    def __init__(self, url: str | None = None) -> None:
        self._url = url or settings.database_url
        self._engine = None
        self._session_factory: async_sessionmaker | None = None

    async def init(self) -> None:
        self._engine = create_async_engine(
            self._url,
            echo=False,
            connect_args={"check_same_thread": False},
        )
        async with self._engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        self._session_factory = async_sessionmaker(
            self._engine, expire_on_commit=False
        )
        logger.info(f"[store] initialized | {self._url}")

        # ── One-time schema migrations ────────────────────────────────────────
        # v1: Equity snapshots were stored without per-account isolation before
        # compound broker keys ("oanda_live", "oanda_practice") were introduced.
        # Old rows can't be attributed to the correct account, so we clear the
        # table once and let fresh snapshots accumulate under the right key.
        # No migration needed for trades — they're re-synced from the broker.
        await self._run_schema_migrations()

    async def close(self) -> None:
        if self._engine:
            await self._engine.dispose()

    async def _run_schema_migrations(self) -> None:
        # Reserved for future schema migrations.
        pass

    async def migrate_broker_keys(self, mapping: dict[str, str]) -> None:
        """
        Migrate plain broker keys to compound keys on first startup after the
        compound-key scheme was introduced (e.g. "oanda" → "oanda_live").

        Trades are renamed — they can be re-synced from the broker if wrong.

        Equity snapshots are DELETED for any plain key being migrated because
        we cannot know which account (live vs practice) they were recorded for.
        Old snapshots from the wrong account would corrupt the equity curve.
        Fresh snapshots will accumulate under the correct compound key going
        forward from the keepalive loop.

        mapping — {old_plain_key: new_compound_key}.
        No-ops when old == new (already compound) or no rows match.
        """
        async with self._session_factory() as sess:
            async with sess.begin():
                for old, new in mapping.items():
                    if old == new:
                        continue

                    # Trades: rename so dedup still works and re-sync is optional
                    r1 = await sess.execute(
                        update(TradeRecord)
                        .where(TradeRecord.broker == old)
                        .values(broker=new)
                    )
                    # Equity snapshots: delete — can't tell which account they
                    # came from, and mixing live/practice data corrupts the curve
                    r2 = await sess.execute(
                        delete(EquitySnapshot)
                        .where(EquitySnapshot.broker == old)
                    )
                    t_rows = r1.rowcount
                    e_rows = r2.rowcount
                    if t_rows or e_rows:
                        logger.info(
                            f"[store] broker key migration {old!r} → {new!r}: "
                            f"renamed {t_rows} trades, cleared {e_rows} equity snapshots "
                            f"(fresh snapshots will accumulate under {new!r})"
                        )

    # ── Write helpers ─────────────────────────────────────────────────────────

    async def save_signal(self, signal: KronosSignal) -> None:
        async with self._session_factory() as sess:
            async with sess.begin():
                rec = SignalRecord(
                    symbol=signal.symbol,
                    generated_at=signal.generated_at,
                    timeframe=signal.timeframe,
                    direction=signal.direction.value,
                    confidence=signal.confidence,
                    entry_price=signal.entry_price,
                    atr=signal.atr,
                    volatility=signal.volatility_forecast,
                    forecast_mean=signal.forecast_mean,
                    forecast_lower=signal.forecast_lower,
                    forecast_upper=signal.forecast_upper,
                )
                sess.add(rec)

    async def save_order(self, order: BracketOrder) -> None:
        async with self._session_factory() as sess:
            async with sess.begin():
                rec = OrderRecord(
                    signal_id=order.signal_id,
                    symbol=order.symbol,
                    side=order.side.value,
                    quantity=order.quantity,
                    entry_price=order.entry_price,
                    stop_loss=order.stop_loss,
                    take_profit=order.take_profit,
                    broker=order.broker.value,
                    status=order.status.value,
                    broker_order_id=order.broker_order_id,
                    filled_price=order.filled_price,
                    created_at=order.created_at,
                    filled_at=order.filled_at,
                )
                sess.add(rec)

    async def save_trade_open(
        self,
        order: BracketOrder,
        signal_confidence: float | None = None,
        timeframe: str | None = None,
        broker_key: str | None = None,
    ) -> None:
        """Record a new open trade after a bracket order fills.

        broker_key — optional compound key (e.g. "oanda_live", "alpaca_paper").
        When omitted, falls back to order.broker.value (plain broker name).
        """
        if not order.broker_order_id:
            return
        direction = "long" if order.side.value.lower() in ("buy", "long") else "short"
        r_risked  = abs(order.entry_price - order.stop_loss) if order.stop_loss else None
        async with self._session_factory() as sess:
            async with sess.begin():
                sess.add(TradeRecord(
                    broker_order_id   = order.broker_order_id,
                    symbol            = order.symbol,
                    direction         = direction,
                    quantity          = order.quantity,
                    broker            = broker_key or order.broker.value,
                    timeframe         = timeframe,
                    signal_confidence = signal_confidence,
                    entry_price       = order.filled_price or order.entry_price,
                    planned_sl        = order.stop_loss,
                    planned_tp        = order.take_profit,
                    entry_datetime    = order.filled_at or datetime.now(tz=timezone.utc),
                    r_risked          = r_risked,
                ))

    async def update_trade_exit(
        self,
        broker_order_id: str,
        exit_price: float,
        exit_reason: str,
        realized_pnl: float,
        exit_dt: datetime | None = None,
    ) -> None:
        """Populate exit fields on a TradeRecord when the position closes."""
        if not broker_order_id:
            return
        exit_dt = exit_dt or datetime.now(tz=timezone.utc)
        async with self._session_factory() as sess:
            async with sess.begin():
                result = await sess.execute(
                    select(TradeRecord)
                    .where(TradeRecord.broker_order_id == broker_order_id)
                    .where(TradeRecord.exit_datetime.is_(None))
                    .limit(1)
                )
                rec = result.scalar_one_or_none()
                if rec is None:
                    return
                rec.exit_price      = exit_price
                rec.exit_datetime   = exit_dt
                rec.exit_reason     = exit_reason
                rec.realized_pnl    = realized_pnl
                rec.is_winner       = realized_pnl > 0
                duration = int((exit_dt - rec.entry_datetime).total_seconds()) if rec.entry_datetime else None
                rec.duration_seconds = duration
                if rec.r_risked and rec.r_risked > 0 and rec.quantity:
                    rec.rr_achieved = realized_pnl / (rec.r_risked * rec.quantity)

    async def close_open_trades_for_symbol(
        self,
        symbol: str,
        exit_price: float,
        realized_pnl: float,
        exit_reason: str = "unknown",
    ) -> None:
        """Close any open TradeRecords for a symbol (fallback for OANDA server-side exits)."""
        exit_dt = datetime.now(tz=timezone.utc)
        async with self._session_factory() as sess:
            async with sess.begin():
                result = await sess.execute(
                    select(TradeRecord)
                    .where(TradeRecord.symbol == symbol)
                    .where(TradeRecord.exit_datetime.is_(None))
                )
                for rec in result.scalars().all():
                    rec.exit_price       = exit_price
                    rec.exit_datetime    = exit_dt
                    rec.exit_reason      = exit_reason
                    rec.realized_pnl     = realized_pnl
                    rec.is_winner        = realized_pnl > 0
                    if rec.entry_datetime:
                        rec.duration_seconds = int((exit_dt - rec.entry_datetime).total_seconds())
                    if rec.r_risked and rec.r_risked > 0 and rec.quantity:
                        rec.rr_achieved = realized_pnl / (rec.r_risked * rec.quantity)

    async def save_equity_snapshot(
        self,
        broker: str,
        equity: float,
        cash: float,
        daily_pnl: float,
        unreal_pnl: float,
    ) -> None:
        async with self._session_factory() as sess:
            async with sess.begin():
                sess.add(EquitySnapshot(
                    timestamp=datetime.now(tz=timezone.utc),
                    broker=broker,
                    equity=equity,
                    cash=cash,
                    daily_pnl=daily_pnl,
                    unreal_pnl=unreal_pnl,
                ))

    # ── Read helpers ──────────────────────────────────────────────────────────

    async def recent_signals(self, symbol: str | None = None, limit: int = 50) -> list[dict]:
        async with self._session_factory() as sess:
            q = select(SignalRecord).order_by(SignalRecord.generated_at.desc()).limit(limit)
            if symbol:
                q = q.where(SignalRecord.symbol == symbol)
            result = await sess.execute(q)
            rows = result.scalars().all()
            return [
                {
                    "id":           r.id,
                    "symbol":       r.symbol,
                    "generated_at": r.generated_at.isoformat(),
                    "timeframe":    r.timeframe,
                    "direction":    r.direction,
                    "confidence":   r.confidence,
                    "entry_price":  r.entry_price,
                }
                for r in rows
            ]

    async def upsert_trade_from_history(
        self,
        broker_order_id: str,
        symbol: str,
        direction: str,
        quantity: float,
        broker: str,
        entry_price: float,
        exit_price: float,
        entry_datetime: datetime,
        exit_datetime: datetime,
        realized_pnl: float,
        exit_reason: str = "unknown",
        planned_sl: float | None = None,
        planned_tp: float | None = None,
        duration_seconds: int | None = None,
        r_risked: float | None = None,
        rr_achieved: float | None = None,
        is_winner: bool | None = None,
        timeframe: str | None = None,
    ) -> bool:
        """Insert a historical trade if not already present. Returns True if inserted.

        Dedup key is (broker_order_id, broker) — the same order ID under a different
        account key (e.g. old plain "oanda" vs new compound "oanda_live") is treated
        as a distinct record so re-syncing after the key scheme change works correctly.
        """
        async with self._session_factory() as sess:
            async with sess.begin():
                existing = await sess.execute(
                    select(TradeRecord)
                    .where(TradeRecord.broker_order_id == broker_order_id)
                    .where(TradeRecord.broker == broker)
                    .limit(1)
                )
                if existing.scalar_one_or_none() is not None:
                    return False
                sess.add(TradeRecord(
                    broker_order_id  = broker_order_id,
                    symbol           = symbol,
                    direction        = direction,
                    quantity         = quantity,
                    broker           = broker,
                    timeframe        = timeframe,
                    entry_price      = entry_price,
                    exit_price       = exit_price,
                    planned_sl       = planned_sl,
                    planned_tp       = planned_tp,
                    entry_datetime   = entry_datetime,
                    exit_datetime    = exit_datetime,
                    exit_reason      = exit_reason,
                    realized_pnl     = realized_pnl,
                    duration_seconds = duration_seconds,
                    r_risked         = r_risked,
                    rr_achieved      = rr_achieved,
                    is_winner        = is_winner if is_winner is not None else realized_pnl > 0,
                ))
                return True

    async def recent_orders(self, limit: int = 100) -> list[dict]:
        async with self._session_factory() as sess:
            result = await sess.execute(
                select(OrderRecord)
                .order_by(OrderRecord.created_at.desc())
                .limit(limit)
            )
            rows = result.scalars().all()
            return [
                {
                    "id": r.id,
                    "symbol": r.symbol,
                    "side": r.side,
                    "quantity": r.quantity,
                    "status": r.status,
                    "broker": r.broker,
                    "broker_order_id": r.broker_order_id,
                    "realized_pnl": r.realized_pnl,
                    "created_at": r.created_at.isoformat() if r.created_at else None,
                }
                for r in rows
            ]

    async def journal_trades(
        self,
        limit: int = 200,
        symbol: str | None = None,
        closed_only: bool = True,
    ) -> list[dict]:
        """Return trade records for the journal view, newest first."""
        async with self._session_factory() as sess:
            q = select(TradeRecord).order_by(TradeRecord.entry_datetime.desc()).limit(limit)
            if symbol:
                q = q.where(TradeRecord.symbol == symbol)
            if closed_only:
                q = q.where(TradeRecord.exit_datetime.isnot(None))
            result = await sess.execute(q)
            return [
                {
                    "id":                 r.id,
                    "symbol":             r.symbol,
                    "direction":          r.direction,
                    "quantity":           r.quantity,
                    "broker":             r.broker,
                    "timeframe":          r.timeframe,
                    "signal_confidence":  r.signal_confidence,
                    "entry_price":        r.entry_price,
                    "exit_price":         r.exit_price,
                    "planned_sl":         r.planned_sl,
                    "planned_tp":         r.planned_tp,
                    "entry_datetime":     r.entry_datetime.isoformat() if r.entry_datetime else None,
                    "exit_datetime":      r.exit_datetime.isoformat()  if r.exit_datetime  else None,
                    "exit_reason":        r.exit_reason,
                    "realized_pnl":       r.realized_pnl,
                    "duration_seconds":   r.duration_seconds,
                    "rr_achieved":        r.rr_achieved,
                    "is_winner":          r.is_winner,
                }
                for r in result.scalars().all()
            ]

    async def journal_stats(self) -> dict:
        """Aggregate statistics for the journal dashboard."""
        async with self._session_factory() as sess:
            result = await sess.execute(
                select(TradeRecord).where(TradeRecord.exit_datetime.isnot(None))
            )
            trades = result.scalars().all()

        if not trades:
            return {
                "total_trades": 0, "winners": 0, "losers": 0,
                "win_rate": 0.0, "total_pnl": 0.0,
                "gross_profit": 0.0, "gross_loss": 0.0,
                "profit_factor": 0.0,
                "avg_winner": 0.0, "avg_loser": 0.0,
                "best_trade": 0.0, "worst_trade": 0.0,
                "avg_duration_seconds": 0,
                "avg_rr_achieved": 0.0,
            }

        pnls      = [t.realized_pnl for t in trades if t.realized_pnl is not None]
        winners   = [p for p in pnls if p > 0]
        losers    = [p for p in pnls if p <= 0]
        gross_p   = sum(winners)
        gross_l   = abs(sum(losers))
        durations = [t.duration_seconds for t in trades if t.duration_seconds]
        rrs       = [t.rr_achieved for t in trades if t.rr_achieved is not None]

        return {
            "total_trades":        len(trades),
            "winners":             len(winners),
            "losers":              len(losers),
            "win_rate":            len(winners) / len(trades) if trades else 0.0,
            "total_pnl":           sum(pnls),
            "gross_profit":        gross_p,
            "gross_loss":          gross_l,
            "profit_factor":       gross_p / gross_l if gross_l else 0.0,
            "avg_winner":          sum(winners) / len(winners) if winners else 0.0,
            "avg_loser":           sum(losers)  / len(losers)  if losers  else 0.0,
            "best_trade":          max(pnls) if pnls else 0.0,
            "worst_trade":         min(pnls) if pnls else 0.0,
            "avg_duration_seconds": int(sum(durations) / len(durations)) if durations else 0,
            "avg_rr_achieved":     sum(rrs) / len(rrs) if rrs else 0.0,
        }

    async def equity_curve(self, limit: int = 500) -> list[dict]:
        async with self._session_factory() as sess:
            result = await sess.execute(
                select(EquitySnapshot)
                .order_by(EquitySnapshot.timestamp.asc())
                .limit(limit)
            )
            rows = result.scalars().all()
            return [
                {
                    "timestamp": r.timestamp.isoformat(),
                    "equity": r.equity,
                    "daily_pnl": r.daily_pnl,
                }
                for r in rows
            ]

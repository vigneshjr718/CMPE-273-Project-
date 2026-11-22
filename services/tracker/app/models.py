"""Tracker read model. Only the tracker reads or writes these tables."""
from datetime import datetime
from typing import Any

from sqlalchemy import (JSON, BigInteger, Boolean, CheckConstraint, DateTime, ForeignKey,
                        Index, Integer, String)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

TS = DateTime(timezone=True)
AUTO_ID = BigInteger().with_variant(Integer, "sqlite")


class Base(DeclarativeBase):
    pass


class OrderStatus(Base):
    """One row per order: latest projected order-level state."""
    __tablename__ = "order_status"

    order_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    status: Mapped[str] = mapped_column(String(40))
    order_created_seen: Mapped[bool] = mapped_column(Boolean)
    creation_event_id: Mapped[str | None] = mapped_column(String(128))
    created_at: Mapped[datetime | None] = mapped_column(TS)
    # Highest coordinator state_version applied (0 = only creation seen).
    state_version: Mapped[int] = mapped_column(Integer)
    last_event_id: Mapped[str] = mapped_column(String(128))
    last_updated: Mapped[datetime] = mapped_column(TS)  # occurred_at of last applied event (display only)
    projected_at: Mapped[datetime] = mapped_column(TS)  # tracker wall clock

    __table_args__ = (CheckConstraint("state_version >= 0", name="ck_order_status_version"),)


class LineStatus(Base):
    """One row per order line: latest projected line state and current seller attempt."""
    __tablename__ = "line_status"

    order_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("order_status.order_id", ondelete="CASCADE"), primary_key=True)
    line_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    part_id: Mapped[str | None] = mapped_column(String(128))
    quantity: Mapped[int | None] = mapped_column(Integer)
    needed_by: Mapped[datetime | None] = mapped_column(TS)
    # "order" once A's order.created was applied (authoritative creation facts),
    # otherwise "coordinator" (copied from B's events until A's arrives).
    creation_source: Mapped[str] = mapped_column(String(20))
    status: Mapped[str] = mapped_column(String(40))
    reason: Mapped[str | None] = mapped_column(String(80))
    current_attempt_id: Mapped[str | None] = mapped_column(String(128))
    seller_id: Mapped[str | None] = mapped_column(String(128))
    estimated_delivery_at: Mapped[datetime | None] = mapped_column(TS)
    delivery_status: Mapped[str | None] = mapped_column(String(40))
    delivered_at: Mapped[datetime | None] = mapped_column(TS)
    attempt_count: Mapped[int] = mapped_column(Integer)
    last_updated: Mapped[datetime] = mapped_column(TS)


class StatusHistory(Base):
    """Every APPLIED event (append-only). Attempt history is derived from it."""
    __tablename__ = "status_history"

    id: Mapped[int] = mapped_column(AUTO_ID, primary_key=True, autoincrement=True)
    event_id: Mapped[str] = mapped_column(String(128), unique=True)
    order_id: Mapped[str] = mapped_column(String(128))
    state_version: Mapped[int] = mapped_column(Integer)  # 0 for order.created
    event_type: Mapped[str] = mapped_column(String(80))
    producer: Mapped[str] = mapped_column(String(40))
    line_id: Mapped[str | None] = mapped_column(String(128))
    attempt_id: Mapped[str | None] = mapped_column(String(128))
    seller_id: Mapped[str | None] = mapped_column(String(128))
    line_status: Mapped[str | None] = mapped_column(String(40))
    order_status: Mapped[str] = mapped_column(String(40))
    reason: Mapped[str | None] = mapped_column(String(80))
    attempt_outcome: Mapped[str | None] = mapped_column(String(20))
    occurred_at: Mapped[datetime] = mapped_column(TS)
    applied_at: Mapped[datetime] = mapped_column(TS)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON)

    __table_args__ = (
        Index("ix_status_history_order", "order_id", "state_version"),
        Index("ix_status_history_line", "order_id", "line_id", "attempt_id"),
    )


class ProcessedEvent(Base):
    """Inbox: every consumed event_id, written in the same transaction as its effect."""
    __tablename__ = "processed_events"

    event_id: Mapped[str] = mapped_column(String(160), primary_key=True)
    topic: Mapped[str] = mapped_column(String(128))
    kafka_partition: Mapped[int] = mapped_column(Integer)
    kafka_offset: Mapped[int] = mapped_column(BigInteger)
    event_type: Mapped[str | None] = mapped_column(String(80))
    producer: Mapped[str | None] = mapped_column(String(40))
    order_id: Mapped[str | None] = mapped_column(String(128))
    # applied | stale | rejected_authority | duplicate_creation | invalid_skipped
    outcome: Mapped[str] = mapped_column(String(30))
    processed_at: Mapped[datetime] = mapped_column(TS)

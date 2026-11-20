"""Coordinator-owned state; transactions/worker logic enforce behavior."""
from datetime import datetime
from uuid import uuid4

from sqlalchemy import (
    CheckConstraint, DateTime, ForeignKey, Index, Integer, JSON, String,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from app.schemas import utc_now


class Base(DeclarativeBase):
    pass


class Workflow(Base):
    __tablename__ = "workflows"
    order_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    payload: Mapped[dict] = mapped_column(JSON)
    status: Mapped[str] = mapped_column(String(40), default="PENDING")
    state_version: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    __table_args__ = (CheckConstraint("state_version >= 0"),)


class LineAttempt(Base):
    __tablename__ = "line_attempts"
    attempt_id: Mapped[str] = mapped_column(String(36), primary_key=True,
                                           default=lambda: str(uuid4()))
    order_id: Mapped[str] = mapped_column(ForeignKey("workflows.order_id"))
    line_id: Mapped[str] = mapped_column(String(128))
    seller_id: Mapped[str] = mapped_column(String(128))
    attempt_number: Mapped[int] = mapped_column(Integer)
    operation_key: Mapped[str] = mapped_column(String(80), unique=True)
    status: Mapped[str] = mapped_column(String(40), default="RESERVING")
    reservation_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    estimated_delivery_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    response_payload: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    reason: Mapped[str | None] = mapped_column(String(80), nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    next_retry_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    __table_args__ = (
        UniqueConstraint("order_id", "line_id", "attempt_number"),
        CheckConstraint("attempt_number >= 1"),
        CheckConstraint("retry_count >= 0"),
        Index("ix_line_attempts_due", "status", "next_retry_at"),
    )


class WorkflowLine(Base):
    __tablename__ = "workflow_lines"
    order_id: Mapped[str] = mapped_column(ForeignKey("workflows.order_id"), primary_key=True)
    line_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    part_id: Mapped[str] = mapped_column(String(128))
    quantity: Mapped[int] = mapped_column(Integer)
    needed_by: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String(40), default="PENDING")
    # Logical link verified under the order lock; avoids cyclic migration FKs.
    current_attempt_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    attempt_count: Mapped[int] = mapped_column(Integer, default=0)
    reason: Mapped[str | None] = mapped_column(String(80), nullable=True)
    __table_args__ = (
        CheckConstraint("quantity > 0"),
        CheckConstraint("attempt_count >= 0"),
    )


class DeliveryCheck(Base):
    __tablename__ = "delivery_checks"
    attempt_id: Mapped[str] = mapped_column(ForeignKey("line_attempts.attempt_id"), primary_key=True)
    order_id: Mapped[str] = mapped_column(ForeignKey("workflows.order_id"))
    line_id: Mapped[str] = mapped_column(String(128))
    poll_count: Mapped[int] = mapped_column(Integer, default=0)
    failure_count: Mapped[int] = mapped_column(Integer, default=0)
    next_poll_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_success_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[str | None] = mapped_column(String(80))
    status_version: Mapped[int | None] = mapped_column(Integer)
    last_payload: Mapped[dict | None] = mapped_column(JSON)
    __table_args__ = (
        CheckConstraint("poll_count >= 0"),
        CheckConstraint("failure_count >= 0"),
        CheckConstraint("status_version IS NULL OR status_version >= 1"),
        Index("ix_delivery_checks_due", "next_poll_at"),
    )


class ProcessedEvent(Base):
    __tablename__ = "processed_events"
    event_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    order_id: Mapped[str] = mapped_column(ForeignKey("workflows.order_id"))
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class OutboxEvent(Base):
    __tablename__ = "outbox"
    event_id: Mapped[str] = mapped_column(String(36), primary_key=True,
                                         default=lambda: str(uuid4()))
    order_id: Mapped[str] = mapped_column(ForeignKey("workflows.order_id"))
    topic: Mapped[str] = mapped_column(String(128))
    event_type: Mapped[str] = mapped_column(String(80))
    state_version: Mapped[int] = mapped_column(Integer)
    payload: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    __table_args__ = (
        UniqueConstraint("order_id", "state_version"),
        CheckConstraint("state_version >= 1"),
        Index("ix_outbox_pending", "published_at", "created_at"),
    )

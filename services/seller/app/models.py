"""SQLAlchemy Core table definitions.

The schema itself is owned by migrations/*.sql (the source of truth); these
mirror it for typed SELECTs. Stock-changing writes go through the DB functions
in migrations/002_functions.sql (see app/services/).
"""
from __future__ import annotations

from sqlalchemy import JSON, Column, DateTime, Integer, MetaData, Table, Text
from sqlalchemy.dialects.postgresql import UUID

metadata = MetaData()

inventory = Table(
    "inventory", metadata,
    Column("part_id", Text, primary_key=True),
    Column("description", Text, nullable=False),
    Column("unit_price_cents", Integer, nullable=False),
    Column("total_quantity", Integer, nullable=False),
    Column("available_quantity", Integer, nullable=False),
    Column("reserved_quantity", Integer, nullable=False),
    Column("delivered_quantity", Integer, nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)

reservations = Table(
    "reservations", metadata,
    Column("reservation_id", UUID(as_uuid=True), primary_key=True),
    Column("operation_key", Text, nullable=False, unique=True),
    Column("order_id", Text, nullable=False),
    Column("line_id", Text, nullable=False),
    Column("part_id", Text, nullable=False),
    Column("quantity", Integer, nullable=False),
    Column("unit_price_cents", Integer, nullable=False),
    Column("status", Text, nullable=False),
    Column("needed_by", DateTime(timezone=True)),
    Column("delivery_status", Text, nullable=False),
    Column("estimated_delivery_at", DateTime(timezone=True), nullable=False),
    Column("cancel_reason", Text),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    Column("cancelled_at", DateTime(timezone=True)),
    Column("delivered_at", DateTime(timezone=True)),
)

idempotency_records = Table(
    "idempotency_records", metadata,
    Column("operation_key", Text, primary_key=True),
    Column("request_hash", Text),
    Column("request_body", JSON),
    Column("order_id", Text),
    Column("line_id", Text),
    Column("part_id", Text),
    Column("quantity", Integer),
    Column("outcome", Text, nullable=False),
    Column("response_code", Integer, nullable=False),
    Column("error_code", Text),
    Column("error_message", Text),
    Column("reservation_id", UUID(as_uuid=True)),
    Column("status_version", Integer, nullable=False),
    Column("cancellation_key", Text),
    Column("cancellation_receipt", UUID(as_uuid=True)),
    Column("cancelled_at", DateTime(timezone=True)),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)

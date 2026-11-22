"""Tracker read model: order_status, line_status, status_history, processed_events.

Revision ID: 0001_initial
Revises:
"""
import sqlalchemy as sa
from alembic import op

revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None

TS = sa.DateTime(timezone=True)


def upgrade() -> None:
    op.create_table(
        "order_status",
        sa.Column("order_id", sa.String(128), primary_key=True),
        sa.Column("status", sa.String(40), nullable=False),
        sa.Column("order_created_seen", sa.Boolean(), nullable=False),
        sa.Column("creation_event_id", sa.String(128)),
        sa.Column("created_at", TS),
        sa.Column("state_version", sa.Integer(), nullable=False),
        sa.Column("last_event_id", sa.String(128), nullable=False),
        sa.Column("last_updated", TS, nullable=False),
        sa.Column("projected_at", TS, nullable=False),
        sa.CheckConstraint("state_version >= 0", name="ck_order_status_version"),
    )
    op.create_table(
        "line_status",
        sa.Column("order_id", sa.String(128),
                  sa.ForeignKey("order_status.order_id", ondelete="CASCADE"), primary_key=True),
        sa.Column("line_id", sa.String(128), primary_key=True),
        sa.Column("part_id", sa.String(128)),
        sa.Column("quantity", sa.Integer()),
        sa.Column("needed_by", TS),
        sa.Column("creation_source", sa.String(20), nullable=False),
        sa.Column("status", sa.String(40), nullable=False),
        sa.Column("reason", sa.String(80)),
        sa.Column("current_attempt_id", sa.String(128)),
        sa.Column("seller_id", sa.String(128)),
        sa.Column("estimated_delivery_at", TS),
        sa.Column("delivery_status", sa.String(40)),
        sa.Column("delivered_at", TS),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("last_updated", TS, nullable=False),
    )
    op.create_table(
        "status_history",
        sa.Column("id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
                  primary_key=True, autoincrement=True),
        sa.Column("event_id", sa.String(128), nullable=False, unique=True),
        sa.Column("order_id", sa.String(128), nullable=False),
        sa.Column("state_version", sa.Integer(), nullable=False),
        sa.Column("event_type", sa.String(80), nullable=False),
        sa.Column("producer", sa.String(40), nullable=False),
        sa.Column("line_id", sa.String(128)),
        sa.Column("attempt_id", sa.String(128)),
        sa.Column("seller_id", sa.String(128)),
        sa.Column("line_status", sa.String(40)),
        sa.Column("order_status", sa.String(40), nullable=False),
        sa.Column("reason", sa.String(80)),
        sa.Column("attempt_outcome", sa.String(20)),
        sa.Column("occurred_at", TS, nullable=False),
        sa.Column("applied_at", TS, nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
    )
    op.create_index("ix_status_history_order", "status_history", ["order_id", "state_version"])
    op.create_index("ix_status_history_line", "status_history", ["order_id", "line_id", "attempt_id"])
    op.create_table(
        "processed_events",
        sa.Column("event_id", sa.String(160), primary_key=True),
        sa.Column("topic", sa.String(128), nullable=False),
        sa.Column("kafka_partition", sa.Integer(), nullable=False),
        sa.Column("kafka_offset", sa.BigInteger(), nullable=False),
        sa.Column("event_type", sa.String(80)),
        sa.Column("producer", sa.String(40)),
        sa.Column("order_id", sa.String(128)),
        sa.Column("outcome", sa.String(30), nullable=False),
        sa.Column("processed_at", TS, nullable=False),
    )


def downgrade() -> None:
    # Destroys the projection. It can be rebuilt by replaying Kafka with a new consumer group.
    op.drop_table("processed_events")
    op.drop_index("ix_status_history_line", table_name="status_history")
    op.drop_index("ix_status_history_order", table_name="status_history")
    op.drop_table("status_history")
    op.drop_table("line_status")
    op.drop_table("order_status")

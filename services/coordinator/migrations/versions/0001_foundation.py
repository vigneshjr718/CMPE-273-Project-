"""Coordinator-owned tables. No other service database is touched."""
from alembic import op
import sqlalchemy as sa

revision = "0001_foundation"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "workflows",
        sa.Column("order_id", sa.String(128), primary_key=True),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(40), nullable=False),
        sa.Column("state_version", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("state_version >= 0"),
    )
    op.create_table(
        "line_attempts",
        sa.Column("attempt_id", sa.String(36), primary_key=True),
        sa.Column("order_id", sa.String(128), sa.ForeignKey("workflows.order_id"), nullable=False),
        sa.Column("line_id", sa.String(128), nullable=False),
        sa.Column("seller_id", sa.String(128), nullable=False),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column("operation_key", sa.String(80), unique=True, nullable=False),
        sa.Column("status", sa.String(40), nullable=False),
        sa.Column("reservation_id", sa.String(128), nullable=True),
        sa.Column("retry_count", sa.Integer(), nullable=False),
        sa.Column("next_retry_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("order_id", "line_id", "attempt_number"),
        sa.CheckConstraint("attempt_number >= 1"),
        sa.CheckConstraint("retry_count >= 0"),
    )
    op.create_index("ix_line_attempts_due", "line_attempts", ["status", "next_retry_at"])
    op.create_table(
        "processed_events",
        sa.Column("event_id", sa.String(36), primary_key=True),
        sa.Column("order_id", sa.String(128), sa.ForeignKey("workflows.order_id"), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "outbox",
        sa.Column("event_id", sa.String(36), primary_key=True),
        sa.Column("order_id", sa.String(128), sa.ForeignKey("workflows.order_id"), nullable=False),
        sa.Column("topic", sa.String(128), nullable=False),
        sa.Column("event_type", sa.String(80), nullable=False),
        sa.Column("state_version", sa.Integer(), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("order_id", "state_version"),
        sa.CheckConstraint("state_version >= 1"),
    )
    op.create_index("ix_outbox_pending", "outbox", ["published_at", "created_at"])


def downgrade():
    op.drop_index("ix_outbox_pending", table_name="outbox")
    op.drop_table("outbox")
    op.drop_table("processed_events")
    op.drop_index("ix_line_attempts_due", table_name="line_attempts")
    op.drop_table("line_attempts")
    op.drop_table("workflows")

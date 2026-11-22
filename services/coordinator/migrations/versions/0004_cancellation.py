"""Add cancellation intent, retry schedule and terminal fencing evidence."""
from alembic import op
import sqlalchemy as sa

revision = "0004_cancellation"
down_revision = "0003_delivery_polling"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "cancellation_checks",
        sa.Column("attempt_id", sa.String(36), sa.ForeignKey("line_attempts.attempt_id"), primary_key=True),
        sa.Column("order_id", sa.String(128), sa.ForeignKey("workflows.order_id"), nullable=False),
        sa.Column("line_id", sa.String(128), nullable=False),
        sa.Column("cancellation_key", sa.String(80), nullable=False, unique=True),
        sa.Column("status", sa.String(40), nullable=False),
        sa.Column("request_count", sa.Integer(), nullable=False),
        sa.Column("next_retry_at", sa.DateTime(timezone=True)),
        sa.Column("last_error", sa.String(80)),
        sa.Column("status_version", sa.Integer()),
        sa.Column("last_payload", sa.JSON()),
        sa.CheckConstraint("request_count >= 0"),
        sa.CheckConstraint("status_version IS NULL OR status_version >= 1"),
    )
    op.create_index("ix_cancellation_checks_due", "cancellation_checks", ["status", "next_retry_at"])


def downgrade():
    op.drop_index("ix_cancellation_checks_due", table_name="cancellation_checks")
    op.drop_table("cancellation_checks")

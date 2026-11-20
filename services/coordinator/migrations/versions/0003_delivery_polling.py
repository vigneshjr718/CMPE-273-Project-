"""Persist delivery observations and poll schedules; retain existing workflows."""
from alembic import op
import sqlalchemy as sa

revision = "0003_delivery_polling"
down_revision = "0002_reservation_flow"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "delivery_checks",
        sa.Column("attempt_id", sa.String(36), sa.ForeignKey("line_attempts.attempt_id"), primary_key=True),
        sa.Column("order_id", sa.String(128), sa.ForeignKey("workflows.order_id"), nullable=False),
        sa.Column("line_id", sa.String(128), nullable=False),
        sa.Column("poll_count", sa.Integer(), nullable=False),
        sa.Column("failure_count", sa.Integer(), nullable=False),
        sa.Column("next_poll_at", sa.DateTime(timezone=True)),
        sa.Column("last_attempt_at", sa.DateTime(timezone=True)),
        sa.Column("last_success_at", sa.DateTime(timezone=True)),
        sa.Column("last_error", sa.String(80)),
        sa.Column("status_version", sa.Integer()),
        sa.Column("last_payload", sa.JSON()),
        sa.CheckConstraint("poll_count >= 0"),
        sa.CheckConstraint("failure_count >= 0"),
        sa.CheckConstraint("status_version IS NULL OR status_version >= 1"),
    )
    op.create_index("ix_delivery_checks_due", "delivery_checks", ["next_poll_at"])


def downgrade():
    op.drop_index("ix_delivery_checks_due", table_name="delivery_checks")
    op.drop_table("delivery_checks")

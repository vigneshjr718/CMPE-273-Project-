"""Persist per-line progress and reservation evidence; preserve existing orders."""
from alembic import op
import sqlalchemy as sa

revision = "0002_reservation_flow"
down_revision = "0001_foundation"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("line_attempts", sa.Column(
        "estimated_delivery_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("line_attempts", sa.Column("response_payload", sa.JSON(), nullable=True))
    op.add_column("line_attempts", sa.Column("reason", sa.String(80), nullable=True))
    op.create_table(
        "workflow_lines",
        sa.Column("order_id", sa.String(128), sa.ForeignKey("workflows.order_id"), primary_key=True),
        sa.Column("line_id", sa.String(128), primary_key=True),
        sa.Column("part_id", sa.String(128), nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("needed_by", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.String(40), nullable=False),
        sa.Column("current_attempt_id", sa.String(36), nullable=True),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("reason", sa.String(80), nullable=True),
        sa.CheckConstraint("quantity > 0"),
        sa.CheckConstraint("attempt_count >= 0"),
    )


def downgrade():
    op.drop_table("workflow_lines")
    op.drop_column("line_attempts", "reason")
    op.drop_column("line_attempts", "response_payload")
    op.drop_column("line_attempts", "estimated_delivery_at")

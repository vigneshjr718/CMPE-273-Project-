"""Durable scheduling and non-destructive backfill of accepted workflows."""
from alembic import op
import sqlalchemy as sa

revision = "0005_workflow_jobs"
down_revision = "0004_cancellation"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "workflow_jobs",
        sa.Column("order_id", sa.String(128), sa.ForeignKey("workflows.order_id"), primary_key=True),
        sa.Column("status", sa.String(24), nullable=False),
        sa.Column("next_run_at", sa.DateTime(timezone=True)),
        sa.Column("last_started_at", sa.DateTime(timezone=True)),
        sa.Column("last_finished_at", sa.DateTime(timezone=True)),
        sa.Column("run_count", sa.Integer(), nullable=False),
        sa.Column("failure_count", sa.Integer(), nullable=False),
        sa.Column("last_error", sa.String(80)),
        sa.CheckConstraint("run_count >= 0"), sa.CheckConstraint("failure_count >= 0"),
    )
    op.create_index("ix_workflow_jobs_due", "workflow_jobs", ["next_run_at", "order_id"])
    op.execute(sa.text("""
        INSERT INTO workflow_jobs (order_id,status,next_run_at,run_count,failure_count)
        SELECT order_id,
               CASE WHEN status IN ('FULFILLED','UNFULFILLED','PARTIALLY_FULFILLED')
                    THEN 'COMPLETE' ELSE 'READY' END,
               CASE WHEN status IN ('FULFILLED','UNFULFILLED','PARTIALLY_FULFILLED')
                    THEN NULL ELSE updated_at END,
               0,0
        FROM workflows
    """))


def downgrade():
    op.drop_index("ix_workflow_jobs_due", table_name="workflow_jobs")
    op.drop_table("workflow_jobs")

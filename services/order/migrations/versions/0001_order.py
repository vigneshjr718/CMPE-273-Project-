"""Order acceptance, lines, and durable Kafka outbox."""
from alembic import op
import sqlalchemy as sa

revision = "0001_order"
down_revision = None
branch_labels = None
depends_on = None

def upgrade():
    op.create_table("orders",
        sa.Column("order_id", sa.String(128), primary_key=True),
        sa.Column("idempotency_key", sa.String(256), nullable=False, unique=True),
        sa.Column("request_fingerprint", sa.String(64), nullable=False),
        sa.Column("status", sa.String(40), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False))
    op.create_table("order_lines",
        sa.Column("line_id", sa.String(128), primary_key=True),
        sa.Column("order_id", sa.String(128), nullable=False),
        sa.Column("part_id", sa.String(128), nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("needed_by", sa.DateTime(timezone=True), nullable=False))
    op.create_index("ix_order_lines_order_id", "order_lines", ["order_id"])
    op.create_table("outbox_events",
        sa.Column("event_id", sa.String(36), primary_key=True),
        sa.Column("order_id", sa.String(128), nullable=False),
        sa.Column("topic", sa.String(128), nullable=False),
        sa.Column("event_type", sa.String(80), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True)),
        sa.UniqueConstraint("order_id", "event_type"))
    op.create_index("ix_outbox_pending", "outbox_events", ["published_at", "created_at"])

def downgrade():
    op.drop_index("ix_outbox_pending", table_name="outbox_events")
    op.drop_table("outbox_events")
    op.drop_index("ix_order_lines_order_id", table_name="order_lines")
    op.drop_table("order_lines")
    op.drop_table("orders")


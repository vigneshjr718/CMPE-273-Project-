"""Dead-letter table for unparseable Kafka messages.

Revision ID: 0002_dead_letter
Revises: 0001_initial
"""
import sqlalchemy as sa
from alembic import op

revision = "0002_dead_letter"
down_revision = "0001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "dead_letter_events",
        sa.Column("id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
                  primary_key=True, autoincrement=True),
        sa.Column("topic", sa.String(128), nullable=False),
        sa.Column("kafka_partition", sa.Integer(), nullable=False),
        sa.Column("kafka_offset", sa.BigInteger(), nullable=False),
        sa.Column("reason", sa.String(300), nullable=False),
        sa.Column("event_id", sa.String(128)),
        sa.Column("order_id", sa.String(128)),
        sa.Column("raw_value", sa.LargeBinary()),
        sa.Column("raw_size", sa.Integer(), nullable=False),
        sa.Column("truncated", sa.Boolean(), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("topic", "kafka_partition", "kafka_offset",
                            name="uq_dead_letter_position"),
    )
    # Earlier builds recorded skipped messages as synthetic "invalid:..." inbox rows.
    op.execute("DELETE FROM processed_events WHERE outcome = 'invalid_skipped'")


def downgrade() -> None:
    op.drop_table("dead_letter_events")

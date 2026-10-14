from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "payments",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("idempotency_key", sa.String(length=255), nullable=False),
        sa.Column("request_hash", sa.String(length=64), nullable=False),
        sa.Column("amount", sa.Numeric(precision=18, scale=2), nullable=False),
        sa.Column("currency", sa.String(length=16), nullable=False),
        sa.Column("description", sa.String(length=1024), nullable=False),
        sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("failure_reason", sa.String(length=255), nullable=True),
        sa.Column("processing_lease_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("webhook_url", sa.String(length=2048), nullable=False),
        sa.Column("webhook_attempts", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("webhook_last_error", sa.Text(), nullable=True),
        sa.Column("webhook_delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("webhook_lease_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("amount > 0", name=op.f("ck_payments_amount_positive")),
        sa.CheckConstraint("currency IN ('RUB', 'USD', 'EUR')", name=op.f("ck_payments_currency")),
        sa.CheckConstraint(
            "status IN ('pending', 'succeeded', 'failed')", name=op.f("ck_payments_payment_status")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_payments")),
        sa.UniqueConstraint("idempotency_key", name=op.f("uq_payments_idempotency_key")),
    )
    op.create_table(
        "outbox",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("aggregate_id", sa.Uuid(), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("routing_key", sa.String(length=255), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("attempts", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_outbox")),
    )
    op.create_index(op.f("ix_outbox_aggregate_id"), "outbox", ["aggregate_id"])
    op.create_index(
        "ix_outbox_pending",
        "outbox",
        ["created_at"],
        postgresql_where=sa.text("published_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_outbox_pending", table_name="outbox")
    op.drop_index(op.f("ix_outbox_aggregate_id"), table_name="outbox")
    op.drop_table("outbox")
    op.drop_table("payments")

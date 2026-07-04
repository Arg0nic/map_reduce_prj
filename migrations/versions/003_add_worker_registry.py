"""add worker registry

Revision ID: 003_add_worker_registry
Revises: 002_create_task_tracking
Create Date: 2026-06-27 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "003_add_worker_registry"
down_revision = "002_create_task_tracking"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "workers",
        sa.Column("worker_id", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("worker_id"),
    )
    op.create_index("idx_workers_status", "workers", ["status"])
    op.create_index("idx_workers_last_seen_at", "workers", ["last_seen_at"])


def downgrade() -> None:
    op.drop_index("idx_workers_last_seen_at", table_name="workers")
    op.drop_index("idx_workers_status", table_name="workers")
    op.drop_table("workers")

"""create jobs table

Revision ID: 001_create_jobs
Revises:
Create Date: 2026-06-25 00:00:00.000000
"""

from alembic import op


revision = "001_create_jobs"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS jobs (
            job_id TEXT PRIMARY KEY,
            status TEXT,
            original_filename TEXT,
            storage TEXT,
            bucket TEXT,
            chunk_count INTEGER,
            total_bytes BIGINT,
            chunks JSONB,
            submitted_at TIMESTAMPTZ,
            updated_at TIMESTAMPTZ,
            completed_at TIMESTAMPTZ,
            result_key TEXT,
            planner_status TEXT,
            planner_message TEXT
        )
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs (status)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_jobs_submitted_at ON jobs (submitted_at)")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_jobs_submitted_at")
    op.execute("DROP INDEX IF EXISTS idx_jobs_status")
    op.execute("DROP TABLE IF EXISTS jobs")

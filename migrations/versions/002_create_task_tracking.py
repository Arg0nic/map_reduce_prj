"""create task tracking tables

Revision ID: 002_create_task_tracking
Revises: 001_create_jobs
Create Date: 2026-06-25 00:00:01.000000
"""

from alembic import op


revision = "002_create_task_tracking"
down_revision = "001_create_jobs"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS tasks (
            task_id TEXT PRIMARY KEY,
            job_id TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
            type TEXT NOT NULL,
            status TEXT NOT NULL,
            address TEXT NOT NULL,
            storage TEXT NOT NULL,
            bucket TEXT NOT NULL,
            part_num INTEGER,
            created_at TIMESTAMPTZ NOT NULL,
            published_at TIMESTAMPTZ,
            started_at TIMESTAMPTZ,
            completed_at TIMESTAMPTZ,
            worker_id TEXT,
            attempts INTEGER NOT NULL DEFAULT 0,
            error_message TEXT,
            updated_at TIMESTAMPTZ NOT NULL
        )
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS idx_tasks_job_id ON tasks (job_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks (status)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_tasks_type ON tasks (type)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_tasks_running_timeout ON tasks (status, started_at)")

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS task_events (
            event_id TEXT PRIMARY KEY,
            job_id TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
            task_id TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
            event_type TEXT NOT NULL,
            task_type TEXT NOT NULL,
            worker_id TEXT,
            message TEXT,
            payload JSONB,
            created_at TIMESTAMPTZ NOT NULL
        )
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS idx_task_events_job_id ON task_events (job_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_task_events_task_id ON task_events (task_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_task_events_event_type ON task_events (event_type)")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_task_events_event_type")
    op.execute("DROP INDEX IF EXISTS idx_task_events_task_id")
    op.execute("DROP INDEX IF EXISTS idx_task_events_job_id")
    op.execute("DROP TABLE IF EXISTS task_events")

    op.execute("DROP INDEX IF EXISTS idx_tasks_running_timeout")
    op.execute("DROP INDEX IF EXISTS idx_tasks_type")
    op.execute("DROP INDEX IF EXISTS idx_tasks_status")
    op.execute("DROP INDEX IF EXISTS idx_tasks_job_id")
    op.execute("DROP TABLE IF EXISTS tasks")

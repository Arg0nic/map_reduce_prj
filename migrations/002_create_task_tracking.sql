CREATE TABLE IF NOT EXISTS tasks (
    task_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    type TEXT NOT NULL,
    status TEXT NOT NULL,
    address TEXT NOT NULL,
    storage TEXT NOT NULL,
    bucket TEXT NOT NULL,
    part_num INTEGER,
    created_at DOUBLE PRECISION NOT NULL,
    published_at DOUBLE PRECISION,
    completed_at DOUBLE PRECISION,
    worker_id TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    error_message TEXT,
    updated_at DOUBLE PRECISION NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tasks_job_id ON tasks (job_id);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks (status);
CREATE INDEX IF NOT EXISTS idx_tasks_type ON tasks (type);

CREATE TABLE IF NOT EXISTS task_events (
    event_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    task_id TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    task_type TEXT NOT NULL,
    worker_id TEXT,
    message TEXT,
    payload JSONB,
    created_at DOUBLE PRECISION NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_task_events_job_id ON task_events (job_id);
CREATE INDEX IF NOT EXISTS idx_task_events_task_id ON task_events (task_id);
CREATE INDEX IF NOT EXISTS idx_task_events_event_type ON task_events (event_type);

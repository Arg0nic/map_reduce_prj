CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,
    status TEXT,
    original_filename TEXT,
    storage TEXT,
    bucket TEXT,
    chunk_count INTEGER,
    total_bytes BIGINT,
    chunks JSONB,
    submitted_at DOUBLE PRECISION,
    updated_at DOUBLE PRECISION,
    completed_at DOUBLE PRECISION,
    result_key TEXT,
    planner_status TEXT,
    planner_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs (status);
CREATE INDEX IF NOT EXISTS idx_jobs_submitted_at ON jobs (submitted_at);

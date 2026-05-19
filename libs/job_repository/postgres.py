import time

from sqlalchemy import BigInteger, Column, DateTime, Integer, MetaData, String, Table, create_engine, select
from sqlalchemy.dialects.postgresql import JSONB, insert

from libs.db_time import decode_timestamp_fields, encode_timestamp_fields

from .base import AbstractJobRepository


JOB_TIMESTAMP_COLUMNS = {"submitted_at", "updated_at", "completed_at"}

JOB_TABLE_COLUMNS = [
    "job_id",
    "status",
    "original_filename",
    "storage",
    "bucket",
    "chunk_count",
    "total_bytes",
    "chunks",
    "submitted_at",
    "updated_at",
    "completed_at",
    "result_key",
    "planner_status",
    "planner_message",
]


def _create_jobs_table():
    metadata = MetaData()
    return Table(
        "jobs",
        metadata,
        Column("job_id", String, primary_key=True),
        Column("status", String),
        Column("original_filename", String),
        Column("storage", String),
        Column("bucket", String),
        Column("chunk_count", Integer),
        Column("total_bytes", BigInteger),
        Column("chunks", JSONB),
        Column("submitted_at", DateTime(timezone=True)),
        Column("updated_at", DateTime(timezone=True)),
        Column("completed_at", DateTime(timezone=True)),
        Column("result_key", String),
        Column("planner_status", String),
        Column("planner_message", String),
    )


class PostgresJobRepository(AbstractJobRepository):
    """Stores job metadata in PostgreSQL."""

    def __init__(self, database_url: str):
        self.database_url = database_url
        self.engine = create_engine(database_url)
        self.jobs = _create_jobs_table()
        self._check_connection()

    def _check_connection(self) -> None:
        try:
            with self.engine.connect() as connection:
                connection.exec_driver_sql("SELECT 1")
        except Exception as exc:
            raise RuntimeError("Failed to connect to PostgreSQL.") from exc

    def _job_payload(self, job: dict) -> dict:
        return {key: job[key] for key in JOB_TABLE_COLUMNS if key in job}

    def _job_db_payload(self, job: dict) -> dict:
        return encode_timestamp_fields(job, JOB_TIMESTAMP_COLUMNS)

    def _row_to_job(self, row) -> dict:
        return decode_timestamp_fields(dict(row), JOB_TIMESTAMP_COLUMNS)

    def save(self, job: dict) -> dict:
        # Keep the repository contract aligned with LocalJsonJobRepository:
        # save mutates the outgoing metadata with a fresh updated_at timestamp.
        payload = self._job_payload(job)
        payload["updated_at"] = time.time()

        if not payload.get("job_id"):
            raise ValueError("Job metadata must include job_id.")

        db_payload = self._job_db_payload(payload)
        statement = insert(self.jobs).values(**db_payload)
        update_values = {
            key: getattr(statement.excluded, key)
            for key in db_payload
            if key != "job_id"
        }
        statement = statement.on_conflict_do_update(
            index_elements=[self.jobs.c.job_id],
            set_=update_values,
        )

        with self.engine.begin() as connection:
            connection.execute(statement)

        return self.load(payload["job_id"]) or payload

    def load(self, job_id: str) -> dict | None:
        statement = select(self.jobs).where(self.jobs.c.job_id == job_id)

        with self.engine.connect() as connection:
            row = connection.execute(statement).mappings().first()

        if row is None:
            return None

        return self._row_to_job(row)

    def update(self, job_id: str, patch: dict) -> dict | None:
        if self.load(job_id) is None:
            return None

        payload = self._job_payload(patch)
        payload["updated_at"] = time.time()

        db_payload = self._job_db_payload(payload)
        statement = (
            self.jobs.update()
            .where(self.jobs.c.job_id == job_id)
            .values(**db_payload)
        )

        with self.engine.begin() as connection:
            connection.execute(statement)

        return self.load(job_id)

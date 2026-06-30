import time
from typing import cast

from sqlalchemy import Table, create_engine, select
from sqlalchemy.dialects.postgresql import insert

from libs.db_models import WorkerRecord
from libs.db_time import decode_timestamp_fields, encode_timestamp_fields, timestamp_to_datetime
from libs.models import WorkerHeartbeat

from .base import AbstractWorkerRepository


WORKER_STATUS_IDLE = "idle"
WORKER_STATUS_RUNNING = "running"
WORKER_STATUS_OFFLINE = "offline"
WORKER_TIMESTAMP_COLUMNS = {"first_seen_at", "last_seen_at", "updated_at"}


def _create_workers_table() -> Table:
    return cast(Table, WorkerRecord.__table__)


class PostgresWorkerRepository(AbstractWorkerRepository):
    """Stores planner-visible worker liveness metadata in PostgreSQL."""

    def __init__(self, database_url: str):
        self.database_url = database_url
        self.engine = create_engine(database_url)
        self.workers = _create_workers_table()
        self._check_connection()

    def _check_connection(self) -> None:
        try:
            with self.engine.connect() as connection:
                connection.exec_driver_sql("SELECT 1")
        except Exception as exc:
            raise RuntimeError("Failed to connect to PostgreSQL.") from exc

    def _worker_db_payload(self, worker: dict) -> dict:
        return encode_timestamp_fields(worker, WORKER_TIMESTAMP_COLUMNS)

    def _row_to_worker(self, row) -> dict:
        return decode_timestamp_fields(dict(row), WORKER_TIMESTAMP_COLUMNS)

    def heartbeat_to_row(self, heartbeat: WorkerHeartbeat) -> dict:
        worker_id = heartbeat.worker_id
        if not worker_id:
            raise ValueError("Worker heartbeat is missing worker_id.")

        seen_at = heartbeat.ts
        current_task = heartbeat.current_task
        if current_task is not None:
            status = WORKER_STATUS_RUNNING
        else:
            status = WORKER_STATUS_IDLE

        return {
            "worker_id": worker_id,
            "status": status,
            "first_seen_at": seen_at,
            "last_seen_at": seen_at,
            "updated_at": seen_at,
        }

    def record_heartbeat(self, heartbeat: WorkerHeartbeat) -> None:
        row = self.heartbeat_to_row(heartbeat)
        db_row = self._worker_db_payload(row)
        statement = insert(self.workers).values(**db_row)
        update_values = {
            key: getattr(statement.excluded, key)
            for key in db_row
            if key not in {"worker_id", "first_seen_at"}
        }
        statement = statement.on_conflict_do_update(
            index_elements=[self.workers.c.worker_id],
            set_=update_values,
        )

        with self.engine.begin() as connection:
            connection.execute(statement)

    def mark_workers_offline(self, cutoff_timestamp: float) -> int:
        cutoff = timestamp_to_datetime(cutoff_timestamp)
        updated_at = time.time()
        statement = (
            self.workers.update()
            .where(
                self.workers.c.status != WORKER_STATUS_OFFLINE,
                self.workers.c.last_seen_at <= cutoff,
            )
            .values(**self._worker_db_payload({
                "status": WORKER_STATUS_OFFLINE,
                "updated_at": updated_at,
            }))
        )

        with self.engine.begin() as connection:
            result = connection.execute(statement)
            return result.rowcount

    def list_workers(self) -> list[dict]:
        statement = select(self.workers).order_by(self.workers.c.worker_id)
        with self.engine.connect() as connection:
            return [self._row_to_worker(row) for row in connection.execute(statement).mappings()]

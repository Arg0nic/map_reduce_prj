import time
import uuid

from sqlalchemy import Column, DateTime, Integer, MetaData, String, Table, create_engine, select
from sqlalchemy.dialects.postgresql import JSONB, insert

from libs.db_time import decode_timestamp_fields, encode_timestamp_fields
from libs.models import TaskCompletedEvent, WorkerTask

from .base import AbstractTaskRepository


TASK_TIMESTAMP_COLUMNS = {"created_at", "published_at", "completed_at", "updated_at"}
TASK_EVENT_TIMESTAMP_COLUMNS = {"created_at"}

TASK_STATUS_PUBLISHED = "published"
TASK_STATUS_COMPLETED = "completed"
TASK_STATUS_FAILED = "failed"
TASK_EVENT_PUBLISHED = "published"
TASK_EVENT_COMPLETED = "completed"
TASK_EVENT_DEAD_LETTERED = "dead_lettered"


def _create_task_tables():
    metadata = MetaData()
    tasks = Table(
        "tasks",
        metadata,
        Column("task_id", String, primary_key=True),
        Column("job_id", String),
        Column("type", String),
        Column("status", String),
        Column("address", String),
        Column("storage", String),
        Column("bucket", String),
        Column("part_num", Integer),
        Column("created_at", DateTime(timezone=True)),
        Column("published_at", DateTime(timezone=True)),
        Column("completed_at", DateTime(timezone=True)),
        Column("worker_id", String),
        Column("attempts", Integer),
        Column("error_message", String),
        Column("updated_at", DateTime(timezone=True)),
    )
    task_events = Table(
        "task_events",
        metadata,
        Column("event_id", String, primary_key=True),
        Column("job_id", String),
        Column("task_id", String),
        Column("event_type", String),
        Column("task_type", String),
        Column("worker_id", String),
        Column("message", String),
        Column("payload", JSONB),
        Column("created_at", DateTime(timezone=True)),
    )
    return tasks, task_events


class PostgresTaskRepository(AbstractTaskRepository):
    """Stores planner-visible worker task lifecycle metadata in PostgreSQL."""

    def __init__(self, database_url: str):
        self.database_url = database_url
        self.engine = create_engine(database_url)
        self.tasks, self.task_events = _create_task_tables()
        self._check_connection()

    def _check_connection(self) -> None:
        try:
            with self.engine.connect() as connection:
                connection.exec_driver_sql("SELECT 1")
        except Exception as exc:
            raise RuntimeError("Failed to connect to PostgreSQL.") from exc

    def _event_payload(
        self,
        event_type: str,
        job_id: str,
        task_id: str,
        task_type: str,
        payload: dict,
        worker_id: str | None = None,
        message: str | None = None,
        created_at: float | None = None,
    ) -> dict:
        return {
            "event_id": str(uuid.uuid4()),
            "job_id": job_id,
            "task_id": task_id,
            "event_type": event_type,
            "task_type": task_type,
            "worker_id": worker_id,
            "message": message,
            "payload": payload,
            "created_at": created_at or time.time(),
        }

    def _task_db_payload(self, task: dict) -> dict:
        return encode_timestamp_fields(task, TASK_TIMESTAMP_COLUMNS)

    def _event_db_payload(self, event: dict) -> dict:
        return encode_timestamp_fields(event, TASK_EVENT_TIMESTAMP_COLUMNS)

    def _row_to_task(self, row) -> dict:
        return decode_timestamp_fields(dict(row), TASK_TIMESTAMP_COLUMNS)

    def _row_to_event(self, row) -> dict:
        return decode_timestamp_fields(dict(row), TASK_EVENT_TIMESTAMP_COLUMNS)

    def record_tasks_published(self, tasks: list[WorkerTask]) -> None:
        if not tasks:
            return

        published_at = time.time()
        with self.engine.begin() as connection:
            for task in tasks:
                task_payload = task.model_dump(mode="json")
                row = {
                    **task_payload,
                    "type": task_payload["type"],
                    "status": TASK_STATUS_PUBLISHED,
                    "published_at": published_at,
                    "completed_at": None,
                    "worker_id": None,
                    "attempts": 0,
                    "error_message": None,
                    "updated_at": published_at,
                }
                db_row = self._task_db_payload(row)
                statement = insert(self.tasks).values(**db_row)
                update_values = {
                    key: getattr(statement.excluded, key)
                    for key in db_row
                    if key != "task_id"
                }
                connection.execute(
                    statement.on_conflict_do_update(
                        index_elements=[self.tasks.c.task_id],
                        set_=update_values,
                    )
                )
                connection.execute(
                    insert(self.task_events).values(
                        **self._event_db_payload(
                            self._event_payload(
                                event_type=TASK_EVENT_PUBLISHED,
                                job_id=task.job_id,
                                task_id=task.task_id,
                                task_type=task.type.value,
                                payload=task_payload,
                                message="Task published to worker queue.",
                                created_at=published_at,
                            )
                        )
                    )
                )

    def mark_task_completed(self, event: TaskCompletedEvent) -> None:
        payload = event.model_dump(mode="json")
        updated_at = time.time()
        statement = (
            self.tasks.update()
            .where(self.tasks.c.task_id == event.task_id)
            .values(**self._task_db_payload({
                "status": TASK_STATUS_COMPLETED,
                "completed_at": event.completed_at,
                "worker_id": event.worker_id,
                "part_num": event.part_num,
                "updated_at": updated_at,
            }))
        )

        with self.engine.begin() as connection:
            result = connection.execute(statement)
            if result.rowcount == 0:
                raise KeyError(f"Task metadata not found for task {event.task_id}")
            connection.execute(
                insert(self.task_events).values(
                    **self._event_db_payload(
                        self._event_payload(
                            event_type=TASK_EVENT_COMPLETED,
                            job_id=event.job_id,
                            task_id=event.task_id,
                            task_type=event.task_type.value,
                            worker_id=event.worker_id,
                            payload=payload,
                            message="Task completed by worker.",
                            created_at=event.completed_at,
                        )
                    )
                )
            )

    def mark_task_failed(self, task: dict, message: str | None = None) -> None:
        task_id = task.get("task_id")
        job_id = task.get("job_id")
        task_type = task.get("type")
        if not task_id:
            raise ValueError("Dead task message is missing task_id.")
        if not job_id:
            raise ValueError("Dead task message is missing job_id.")
        if not task_type:
            raise ValueError("Dead task message is missing type.")

        updated_at = time.time()
        statement = (
            self.tasks.update()
            .where(self.tasks.c.task_id == task_id)
            .values(**self._task_db_payload({
                "status": TASK_STATUS_FAILED,
                "error_message": message,
                "updated_at": updated_at,
            }))
        )

        with self.engine.begin() as connection:
            result = connection.execute(statement)
            if result.rowcount == 0:
                raise KeyError(f"Task metadata not found for task {task_id}")
            connection.execute(
                insert(self.task_events).values(
                    **self._event_db_payload(
                        self._event_payload(
                            event_type=TASK_EVENT_DEAD_LETTERED,
                            job_id=job_id,
                            task_id=task_id,
                            task_type=task_type,
                            payload=task,
                            message=message,
                            created_at=updated_at,
                        )
                    )
                )
            )

    def list_tasks_for_job(self, job_id: str) -> list[dict]:
        statement = (
            select(self.tasks)
            .where(self.tasks.c.job_id == job_id)
            .order_by(self.tasks.c.created_at, self.tasks.c.task_id)
        )
        with self.engine.connect() as connection:
            return [self._row_to_task(row) for row in connection.execute(statement).mappings()]

    def list_events_for_job(self, job_id: str) -> list[dict]:
        statement = (
            select(self.task_events)
            .where(self.task_events.c.job_id == job_id)
            .order_by(self.task_events.c.created_at, self.task_events.c.event_id)
        )
        with self.engine.connect() as connection:
            return [self._row_to_event(row) for row in connection.execute(statement).mappings()]

from sqlalchemy import DateTime

from libs.db_models import metadata
from libs.job_repository.postgres import _create_jobs_table
from libs.task_repository.postgres import _create_task_tables


def assert_timestamptz(column) -> None:
    assert isinstance(column.type, DateTime)
    assert column.type.timezone is True


def test_job_repository_maps_time_columns_to_timestamptz() -> None:
    jobs = _create_jobs_table()

    assert_timestamptz(jobs.c.submitted_at)
    assert_timestamptz(jobs.c.updated_at)
    assert_timestamptz(jobs.c.completed_at)


def test_task_repository_maps_time_columns_to_timestamptz() -> None:
    tasks, task_events = _create_task_tables()

    assert_timestamptz(tasks.c.created_at)
    assert_timestamptz(tasks.c.published_at)
    assert_timestamptz(tasks.c.started_at)
    assert_timestamptz(tasks.c.completed_at)
    assert_timestamptz(tasks.c.updated_at)
    assert_timestamptz(task_events.c.created_at)


def test_orm_metadata_contains_database_schema() -> None:
    assert {"jobs", "tasks", "task_events"} == set(metadata.tables)

    jobs = metadata.tables["jobs"]
    tasks = metadata.tables["tasks"]
    task_events = metadata.tables["task_events"]

    assert {index.name for index in jobs.indexes} == {
        "idx_jobs_status",
        "idx_jobs_submitted_at",
    }
    assert {index.name for index in tasks.indexes} == {
        "idx_tasks_job_id",
        "idx_tasks_status",
        "idx_tasks_type",
        "idx_tasks_running_timeout",
    }
    assert {index.name for index in task_events.indexes} == {
        "idx_task_events_job_id",
        "idx_task_events_task_id",
        "idx_task_events_event_type",
    }

    task_job_fk = next(iter(tasks.c.job_id.foreign_keys))
    event_job_fk = next(iter(task_events.c.job_id.foreign_keys))
    event_task_fk = next(iter(task_events.c.task_id.foreign_keys))

    assert task_job_fk.column is jobs.c.job_id
    assert event_job_fk.column is jobs.c.job_id
    assert event_task_fk.column is tasks.c.task_id
    assert task_job_fk.ondelete == "CASCADE"
    assert event_job_fk.ondelete == "CASCADE"
    assert event_task_fk.ondelete == "CASCADE"

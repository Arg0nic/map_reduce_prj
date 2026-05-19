from sqlalchemy import DateTime

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
    assert_timestamptz(tasks.c.completed_at)
    assert_timestamptz(tasks.c.updated_at)
    assert_timestamptz(task_events.c.created_at)

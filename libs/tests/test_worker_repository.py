import pytest

from libs.worker_repository.postgres import (
    PostgresWorkerRepository,
    WORKER_STATUS_IDLE,
    WORKER_STATUS_RUNNING,
)
from libs.models import WorkerHeartbeat


def make_repository() -> PostgresWorkerRepository:
    return object.__new__(PostgresWorkerRepository)


def test_heartbeat_to_row_maps_idle_worker() -> None:
    repository = make_repository()

    row = repository.heartbeat_to_row(WorkerHeartbeat(worker_id="worker-1", ts=0))

    assert row == {
        "worker_id": "worker-1",
        "status": WORKER_STATUS_IDLE,
        "first_seen_at": 0,
        "last_seen_at": 0,
        "updated_at": 0,
    }


def test_heartbeat_to_row_maps_running_worker() -> None:
    repository = make_repository()

    row = repository.heartbeat_to_row(WorkerHeartbeat.model_validate({
        "worker_id": "worker-1",
        "ts": 100.0,
        "current_task": {
            "job_id": "job-1",
            "task_id": "map-1",
            "type": "map",
            "started_at": 90.0,
            "part_num": 2,
        },
    }))

    assert row == {
        "worker_id": "worker-1",
        "status": WORKER_STATUS_RUNNING,
        "first_seen_at": 100.0,
        "last_seen_at": 100.0,
        "updated_at": 100.0,
    }


def test_heartbeat_to_row_rejects_missing_worker_id() -> None:
    repository = make_repository()

    with pytest.raises(ValueError):
        WorkerHeartbeat.model_validate({"ts": 100.0})

from types import SimpleNamespace

import pytest

import planner.service as planner_service
from libs.models import JobStatus, JobUploadedEvent, TaskCompletedEvent, TaskType, WorkerHeartbeat
from planner.service import PlannerService


class RecordingTaskRepository:
    def __init__(self):
        self.published = []
        self.started = []
        self.completed = []
        self.failed = []
        self.timed_out_tasks = []
        self.tasks: dict[str, dict] = {}

    def task_to_row(self, task) -> dict:
        if hasattr(task, "model_dump"):
            row = task.model_dump(mode="json")
        else:
            row = dict(vars(task))

        task_type = row.get("type") or row.get("task_type")
        if isinstance(task_type, TaskType):
            task_type = task_type.value

        return {
            **row,
            "type": task_type,
            "status": row.get("status", "published"),
        }

    def add_task(
        self,
        task_id: str,
        task_type: TaskType,
        job_id: str = "job-1",
        status: str = "published",
        bucket: str = "bucket-1",
        part_num: int | None = None,
    ) -> dict:
        task = {
            "job_id": job_id,
            "task_id": task_id,
            "type": task_type.value,
            "status": status,
            "address": "address",
            "storage": "minio",
            "bucket": bucket,
            "part_num": part_num,
        }
        self.tasks[task_id] = task
        return task

    def record_tasks_published(self, tasks):
        published = list(tasks)
        self.published.append(published)
        for task in published:
            row = self.task_to_row(task)
            self.tasks[row["task_id"]] = row

    def mark_task_completed(self, event):
        self.completed.append(event)
        self.tasks[event.task_id]["status"] = "completed"
        self.tasks[event.task_id]["worker_id"] = event.worker_id
        self.tasks[event.task_id]["completed_at"] = event.completed_at
        self.tasks[event.task_id]["part_num"] = event.part_num

    def mark_task_started(self, task, worker_id: str, started_at: float):
        self.started.append((task, worker_id, started_at))

    def mark_task_failed(self, task, message: str | None = None, event_type: str = "failed"):
        self.failed.append((task, message, event_type))

    def list_timed_out_running_tasks(self, cutoff_timestamp: float) -> list[dict]:
        return self.timed_out_tasks

    def list_tasks_for_job(self, job_id: str) -> list[dict]:
        return [dict(task) for task in self.tasks.values() if task["job_id"] == job_id]

    def list_events_for_job(self, job_id: str) -> list[dict]:
        return []


class RecordingJobRepository:
    def __init__(self, jobs: dict[str, dict] | None = None):
        self.updated = []
        self.jobs = jobs or {}

    def save(self, job: dict) -> dict:
        return job

    def load(self, job_id: str) -> dict | None:
        return self.jobs.get(job_id)

    def update(self, job_id: str, patch: dict) -> dict | None:
        self.updated.append((job_id, patch))
        updated = {"job_id": job_id, **self.jobs.get(job_id, {}), **patch}
        self.jobs[job_id] = updated
        return updated


class RecordingWorkerRepository:
    def __init__(self):
        self.heartbeats = []
        self.offline_cutoffs = []
        self.offline_count = 0

    def record_heartbeat(self, heartbeat: WorkerHeartbeat) -> None:
        self.heartbeats.append(heartbeat)

    def mark_workers_offline(self, cutoff_timestamp: float) -> int:
        self.offline_cutoffs.append(cutoff_timestamp)
        return self.offline_count

    def list_workers(self) -> list[dict]:
        return []


def make_completed_event(
    task_id: str,
    task_type: TaskType,
    job_id: str = "job-1",
    part_num: int | None = None,
) -> TaskCompletedEvent:
    return TaskCompletedEvent(
        job_id=job_id,
        task_id=task_id,
        task_type=task_type,
        worker_id="worker-1",
        bucket="bucket-1",
        completed_at=123.45,
        part_num=part_num,
    )


def test_handle_job_uploaded_records_published_map_tasks_and_job_processing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task_repository = RecordingTaskRepository()
    job_repository = RecordingJobRepository({"job-1": {"job_id": "job-1", "status": "uploaded"}})
    service = PlannerService(task_repository=task_repository, job_repository=job_repository)
    event = JobUploadedEvent(
        job_id="job-1",
        bucket="bucket-1",
        chunks_prefix="jobs/job-1/chunks/",
        created_at=123.45,
    )
    tasks = [
        SimpleNamespace(job_id="job-1", task_id="map-1", type=TaskType.MAP, bucket="bucket-1"),
        SimpleNamespace(job_id="job-1", task_id="map-2", type=TaskType.MAP, bucket="bucket-1"),
    ]
    monkeypatch.setattr(planner_service, "create_map_tasks_for_job", lambda ch, received_event: tasks)

    service.handle_job_uploaded(ch=object(), event=event)

    assert task_repository.published == [tasks]
    assert {task["task_id"] for task in task_repository.list_tasks_for_job("job-1")} == {"map-1", "map-2"}
    assert job_repository.updated == [
        (
            "job-1",
            {
                "status": JobStatus.PROCESSING.value,
                "planner_status": "map_running",
                "planner_message": "Planner published 2 map tasks.",
            },
        )
    ]


def test_handle_job_uploaded_skips_already_planned_job(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task_repository = RecordingTaskRepository()
    task_repository.add_task("map-1", TaskType.MAP)
    service = PlannerService(task_repository=task_repository)
    event = JobUploadedEvent(
        job_id="job-1",
        bucket="bucket-1",
        chunks_prefix="jobs/job-1/chunks/",
        created_at=123.45,
    )

    def fail_create_map_tasks(ch, received_event):
        raise AssertionError("map tasks should not be recreated")

    monkeypatch.setattr(planner_service, "create_map_tasks_for_job", fail_create_map_tasks)

    service.handle_job_uploaded(ch=object(), event=event)

    assert task_repository.published == []


def test_start_reduce_phase_records_reduce_tasks_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task_repository = RecordingTaskRepository()
    job_repository = RecordingJobRepository({"job-1": {"job_id": "job-1", "status": "processing"}})
    service = PlannerService(task_repository=task_repository, job_repository=job_repository)
    tasks = [
        SimpleNamespace(job_id="job-1", task_id="reduce-0", type=TaskType.REDUCE, bucket="bucket-1"),
        SimpleNamespace(job_id="job-1", task_id="reduce-1", type=TaskType.REDUCE, bucket="bucket-1"),
    ]
    monkeypatch.setattr(planner_service, "create_reduce_tasks_for_job", lambda ch, job_id, bucket: tasks)

    service.start_reduce_phase(ch=object(), job_id="job-1", bucket="bucket-1")
    service.start_reduce_phase(ch=object(), job_id="job-1", bucket="bucket-1")

    assert task_repository.published == [tasks]
    assert {task["task_id"] for task in task_repository.list_tasks_for_job("job-1")} == {
        "reduce-0",
        "reduce-1",
    }
    assert job_repository.updated == [
        (
            "job-1",
            {
                "status": JobStatus.PROCESSING.value,
                "planner_status": "reduce_running",
                "planner_message": "Planner published 2 reduce tasks.",
            },
        )
    ]


def test_handle_worker_heartbeat_records_current_task() -> None:
    task_repository = RecordingTaskRepository()
    worker_repository = RecordingWorkerRepository()
    service = PlannerService(task_repository=task_repository, worker_repository=worker_repository)
    heartbeat = WorkerHeartbeat.model_validate({
        "worker_id": "worker-1",
        "ts": 101.0,
        "current_task": {
            "job_id": "job-1",
            "task_id": "map-1",
            "type": "map",
            "bucket": "bucket-1",
            "started_at": 100.0,
            "part_num": None,
        },
    })

    service.handle_worker_heartbeat(heartbeat)

    assert worker_repository.heartbeats == [heartbeat]
    assert task_repository.started == [
        (
            {
                "job_id": "job-1",
                "task_id": "map-1",
                "type": "map",
                "part_num": None,
            },
            "worker-1",
            100.0,
        )
    ]


def test_handle_worker_heartbeat_ignores_idle_worker() -> None:
    task_repository = RecordingTaskRepository()
    worker_repository = RecordingWorkerRepository()
    service = PlannerService(task_repository=task_repository, worker_repository=worker_repository)
    heartbeat = WorkerHeartbeat(worker_id="worker-1", ts=101.0)

    service.handle_worker_heartbeat(heartbeat)

    assert worker_repository.heartbeats == [heartbeat]
    assert task_repository.started == []


def test_mark_stale_workers_offline_uses_timeout_cutoff() -> None:
    worker_repository = RecordingWorkerRepository()
    worker_repository.offline_count = 2
    service = PlannerService(worker_repository=worker_repository)

    offline_count = service.mark_stale_workers_offline(timeout_seconds=15, now=100.0)

    assert offline_count == 2
    assert worker_repository.offline_cutoffs == [85.0]


def test_map_completion_starts_reduce_after_all_persisted_maps_complete() -> None:
    task_repository = RecordingTaskRepository()
    task_repository.add_task("map-1", TaskType.MAP)
    task_repository.add_task("map-2", TaskType.MAP)
    service = PlannerService(task_repository=task_repository)
    calls = []

    def fake_start_reduce_phase(ch, job_id, bucket) -> None:
        calls.append((ch, job_id, bucket))
        task_repository.add_task("reduce-0", TaskType.REDUCE)

    service.start_reduce_phase = fake_start_reduce_phase  # type: ignore[method-assign]
    channel = object()

    service.handle_map_completed(channel, make_completed_event("map-1", TaskType.MAP))
    service.handle_map_completed(channel, make_completed_event("map-2", TaskType.MAP))
    service.handle_map_completed(channel, make_completed_event("map-2", TaskType.MAP))

    assert [event.task_id for event in task_repository.completed] == ["map-1", "map-2"]
    assert calls == [(channel, "job-1", "bucket-1")]


def test_map_completion_does_not_start_reduce_when_reduce_tasks_already_exist() -> None:
    task_repository = RecordingTaskRepository()
    task_repository.add_task("map-1", TaskType.MAP)
    task_repository.add_task("reduce-0", TaskType.REDUCE)
    service = PlannerService(task_repository=task_repository)
    calls = []
    service.start_reduce_phase = lambda ch, job_id, bucket: calls.append((job_id, bucket))  # type: ignore[method-assign]

    service.handle_map_completed(object(), make_completed_event("map-1", TaskType.MAP))

    assert [event.task_id for event in task_repository.completed] == ["map-1"]
    assert calls == []


def test_map_completion_ignores_unknown_job_or_task() -> None:
    task_repository = RecordingTaskRepository()
    task_repository.add_task("map-1", TaskType.MAP)
    service = PlannerService(task_repository=task_repository)
    calls = []
    service.start_reduce_phase = lambda ch, job_id, bucket: calls.append((job_id, bucket))  # type: ignore[method-assign]

    service.handle_map_completed(object(), make_completed_event("map-1", TaskType.MAP, job_id="missing-job"))
    service.handle_map_completed(object(), make_completed_event("unknown-map", TaskType.MAP))

    assert task_repository.completed == []
    assert calls == []


def test_reduce_completion_finalizes_after_all_persisted_reduces_complete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task_repository = RecordingTaskRepository()
    task_repository.add_task("reduce-0", TaskType.REDUCE, part_num=0)
    task_repository.add_task("reduce-1", TaskType.REDUCE, part_num=1)
    job_repository = RecordingJobRepository({"job-1": {"job_id": "job-1", "status": "processing"}})
    service = PlannerService(task_repository=task_repository, job_repository=job_repository)
    finalizations = []

    def fake_finalize(job_id, bucket):
        finalizations.append((job_id, bucket))
        job_repository.jobs[job_id]["status"] = JobStatus.DONE.value
        return "jobs/job-1/result/result.json"

    monkeypatch.setattr(planner_service, "finalize_job", fake_finalize)

    service.handle_reduce_completed(make_completed_event("reduce-0", TaskType.REDUCE, part_num=0))
    service.handle_reduce_completed(make_completed_event("reduce-1", TaskType.REDUCE, part_num=1))
    service.handle_reduce_completed(make_completed_event("reduce-1", TaskType.REDUCE, part_num=1))

    assert [event.task_id for event in task_repository.completed] == ["reduce-0", "reduce-1"]
    assert finalizations == [("job-1", "bucket-1")]


def test_reduce_completion_ignores_unknown_job_or_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task_repository = RecordingTaskRepository()
    task_repository.add_task("reduce-0", TaskType.REDUCE)
    service = PlannerService(task_repository=task_repository)
    finalizations = []
    monkeypatch.setattr(planner_service, "finalize_job", lambda job_id, bucket: finalizations.append((job_id, bucket)))

    service.handle_reduce_completed(make_completed_event("reduce-0", TaskType.REDUCE, job_id="missing-job"))
    service.handle_reduce_completed(make_completed_event("unknown-reduce", TaskType.REDUCE))

    assert task_repository.completed == []
    assert finalizations == []


def test_handle_task_completed_routes_by_task_type(monkeypatch: pytest.MonkeyPatch) -> None:
    service = PlannerService()
    calls = []
    service.handle_map_completed = lambda ch, event: calls.append(("map", ch, event.task_id))  # type: ignore[method-assign]
    service.handle_reduce_completed = lambda event: calls.append(("reduce", event.task_id))  # type: ignore[method-assign]
    channel = object()

    service.handle_task_completed(channel, make_completed_event("map-1", TaskType.MAP))
    service.handle_task_completed(channel, make_completed_event("reduce-1", TaskType.REDUCE))

    assert calls == [
        ("map", channel, "map-1"),
        ("reduce", "reduce-1"),
    ]


def test_handle_task_completed_ignores_done_persisted_job() -> None:
    job_repository = RecordingJobRepository({"job-1": {"job_id": "job-1", "status": "done"}})
    service = PlannerService(job_repository=job_repository)
    calls = []
    service.handle_map_completed = lambda ch, event: calls.append(event)  # type: ignore[method-assign]

    service.handle_task_completed(object(), make_completed_event("map-1", TaskType.MAP))

    assert calls == []


def test_handle_task_completed_ignores_failed_persisted_job() -> None:
    job_repository = RecordingJobRepository({"job-1": {"job_id": "job-1", "status": "failed"}})
    service = PlannerService(job_repository=job_repository)
    calls = []
    service.handle_map_completed = lambda ch, event: calls.append(event)  # type: ignore[method-assign]

    service.handle_task_completed(object(), make_completed_event("map-1", TaskType.MAP))

    assert calls == []


def test_handle_task_dead_records_failed_task_and_job(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task_repository = RecordingTaskRepository()
    job_repository = RecordingJobRepository()
    service = PlannerService(
        task_repository=task_repository,
        job_repository=job_repository,
    )
    monkeypatch.setattr(planner_service.time, "time", lambda: 500.0)
    task = {
        "job_id": "job-1",
        "task_id": "map-1",
        "type": "map",
    }

    service.handle_task_dead(task)

    message = "Task map-1 reached dead queue after worker retries."
    assert task_repository.failed == [(task, message, "dead_lettered")]
    assert job_repository.updated == [
        (
            "job-1",
            {
                "status": "failed",
                "completed_at": 500.0,
                "planner_status": "failed",
                "planner_message": message,
            },
        )
    ]


def test_fail_timed_out_tasks_marks_task_and_job_failed() -> None:
    task_repository = RecordingTaskRepository()
    job_repository = RecordingJobRepository()
    service = PlannerService(
        task_repository=task_repository,
        job_repository=job_repository,
    )
    timed_out_task = {
        "job_id": "job-1",
        "task_id": "map-1",
        "type": "map",
        "started_at": 100.0,
    }
    task_repository.timed_out_tasks = [timed_out_task]

    failed_count = service.fail_timed_out_tasks(timeout_seconds=30, now=131.0)

    message = "Task map-1 timed out after 30 seconds."
    assert failed_count == 1
    assert task_repository.failed == [(timed_out_task, message, "timed_out")]
    assert job_repository.updated == [
        (
            "job-1",
            {
                "status": "failed",
                "completed_at": 131.0,
                "planner_status": "failed",
                "planner_message": message,
            },
        )
    ]


@pytest.mark.parametrize(
    "task",
    [
        {"task_id": "map-1", "type": "map"},
        {"job_id": "job-1", "type": "map"},
        {"job_id": "job-1", "task_id": "map-1"},
    ],
)
def test_handle_task_dead_rejects_invalid_dead_task(task: dict) -> None:
    service = PlannerService()

    with pytest.raises(ValueError):
        service.handle_task_dead(task)

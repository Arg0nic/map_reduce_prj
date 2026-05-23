from types import SimpleNamespace

import pytest

import planner.service as planner_service
from libs.models import JobUploadedEvent, TaskCompletedEvent, TaskType
from planner.service import PlannerService
from planner.state import JobPlanState


class RecordingTaskRepository:
    def __init__(self):
        self.published = []
        self.started = []
        self.completed = []
        self.failed = []
        self.timed_out_tasks = []

    def record_tasks_published(self, tasks):
        self.published.append(list(tasks))

    def mark_task_completed(self, event):
        self.completed.append(event)

    def mark_task_started(self, task, worker_id: str, started_at: float):
        self.started.append((task, worker_id, started_at))

    def mark_task_failed(self, task, message: str | None = None, event_type: str = "failed"):
        self.failed.append((task, message, event_type))

    def list_timed_out_running_tasks(self, cutoff_timestamp: float) -> list[dict]:
        return self.timed_out_tasks

    def list_tasks_for_job(self, job_id: str) -> list[dict]:
        return []

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


def test_handle_job_uploaded_creates_initial_job_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = PlannerService()
    event = JobUploadedEvent(
        job_id="job-1",
        bucket="bucket-1",
        chunks_prefix="jobs/job-1/chunks/",
        created_at=123.45,
    )
    tasks = [
        SimpleNamespace(task_id="map-1"),
        SimpleNamespace(task_id="map-2"),
    ]
    monkeypatch.setattr(planner_service, "create_map_tasks_for_job", lambda ch, received_event: tasks)

    service.handle_job_uploaded(ch=object(), event=event)

    assert service.job_states["job-1"] == JobPlanState(
        bucket="bucket-1",
        map_task_ids={"map-1", "map-2"},
    )


def test_handle_job_uploaded_records_published_map_tasks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task_repository = RecordingTaskRepository()
    service = PlannerService(task_repository=task_repository)
    event = JobUploadedEvent(
        job_id="job-1",
        bucket="bucket-1",
        chunks_prefix="jobs/job-1/chunks/",
        created_at=123.45,
    )
    tasks = [
        SimpleNamespace(task_id="map-1"),
        SimpleNamespace(task_id="map-2"),
    ]
    monkeypatch.setattr(planner_service, "create_map_tasks_for_job", lambda ch, received_event: tasks)

    service.handle_job_uploaded(ch=object(), event=event)

    assert task_repository.published == [tasks]


def test_start_reduce_phase_records_reduce_tasks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = JobPlanState(bucket="bucket-1", map_task_ids={"map-1"})
    service = PlannerService({"job-1": state})
    tasks = [
        SimpleNamespace(task_id="reduce-0"),
        SimpleNamespace(task_id="reduce-1"),
    ]
    monkeypatch.setattr(planner_service, "create_reduce_tasks_for_job", lambda ch, job_id, bucket: tasks)

    service.start_reduce_phase(ch=object(), job_id="job-1", state=state)

    assert state.reduce_task_ids == {"reduce-0", "reduce-1"}
    assert state.reduce_started is True


def test_start_reduce_phase_records_published_reduce_tasks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task_repository = RecordingTaskRepository()
    state = JobPlanState(bucket="bucket-1", map_task_ids={"map-1"})
    service = PlannerService({"job-1": state}, task_repository=task_repository)
    tasks = [
        SimpleNamespace(task_id="reduce-0"),
        SimpleNamespace(task_id="reduce-1"),
    ]
    monkeypatch.setattr(planner_service, "create_reduce_tasks_for_job", lambda ch, job_id, bucket: tasks)

    service.start_reduce_phase(ch=object(), job_id="job-1", state=state)

    assert task_repository.published == [tasks]


def test_handle_worker_heartbeat_records_current_task() -> None:
    task_repository = RecordingTaskRepository()
    service = PlannerService(task_repository=task_repository)
    heartbeat = {
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
    }

    service.handle_worker_heartbeat(heartbeat)

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
    service = PlannerService(task_repository=task_repository)

    service.handle_worker_heartbeat({"worker_id": "worker-1", "ts": 101.0})

    assert task_repository.started == []


def test_map_completion_starts_reduce_after_all_known_maps_complete() -> None:
    state = JobPlanState(bucket="bucket-1", map_task_ids={"map-1", "map-2"})
    service = PlannerService({"job-1": state})
    calls = []

    def fake_start_reduce_phase(ch, job_id, received_state) -> None:
        calls.append((ch, job_id, received_state))
        received_state.reduce_started = True

    service.start_reduce_phase = fake_start_reduce_phase  # type: ignore[method-assign]
    channel = object()

    service.handle_map_completed(channel, make_completed_event("map-1", TaskType.MAP))
    service.handle_map_completed(channel, make_completed_event("map-2", TaskType.MAP))
    service.handle_map_completed(channel, make_completed_event("map-2", TaskType.MAP))

    assert state.completed_map_task_ids == {"map-1", "map-2"}
    assert calls == [(channel, "job-1", state)]


def test_map_completion_records_completed_task_once() -> None:
    task_repository = RecordingTaskRepository()
    state = JobPlanState(bucket="bucket-1", map_task_ids={"map-1"})
    service = PlannerService({"job-1": state}, task_repository=task_repository)
    service.start_reduce_phase = lambda ch, job_id, received_state: None  # type: ignore[method-assign]
    event = make_completed_event("map-1", TaskType.MAP)

    service.handle_map_completed(object(), event)
    service.handle_map_completed(object(), event)

    assert task_repository.completed == [event]


def test_map_completion_ignores_unknown_job_or_task() -> None:
    state = JobPlanState(bucket="bucket-1", map_task_ids={"map-1"})
    service = PlannerService({"job-1": state})
    calls = []
    service.start_reduce_phase = lambda ch, job_id, received_state: calls.append((job_id, received_state))  # type: ignore[method-assign]

    service.handle_map_completed(object(), make_completed_event("map-1", TaskType.MAP, job_id="missing-job"))
    service.handle_map_completed(object(), make_completed_event("unknown-map", TaskType.MAP))

    assert state.completed_map_task_ids == set()
    assert calls == []


def test_reduce_completion_finalizes_after_all_known_reduces_complete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = JobPlanState(
        bucket="bucket-1",
        map_task_ids={"map-1"},
        reduce_task_ids={"reduce-0", "reduce-1"},
    )
    service = PlannerService({"job-1": state})
    finalizations = []
    monkeypatch.setattr(
        planner_service,
        "finalize_job",
        lambda job_id, bucket: finalizations.append((job_id, bucket)) or "jobs/job-1/result/result.json",
    )

    service.handle_reduce_completed(make_completed_event("reduce-0", TaskType.REDUCE, part_num=0))
    service.handle_reduce_completed(make_completed_event("reduce-1", TaskType.REDUCE, part_num=1))
    service.handle_reduce_completed(make_completed_event("reduce-1", TaskType.REDUCE, part_num=1))

    assert state.completed_reduce_task_ids == {"reduce-0", "reduce-1"}
    assert state.done is True
    assert finalizations == [("job-1", "bucket-1")]


def test_reduce_completion_records_completed_task_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task_repository = RecordingTaskRepository()
    state = JobPlanState(
        bucket="bucket-1",
        map_task_ids={"map-1"},
        reduce_task_ids={"reduce-0"},
    )
    service = PlannerService({"job-1": state}, task_repository=task_repository)
    monkeypatch.setattr(
        planner_service,
        "finalize_job",
        lambda job_id, bucket: "jobs/job-1/result/result.json",
    )
    event = make_completed_event("reduce-0", TaskType.REDUCE, part_num=0)

    service.handle_reduce_completed(event)
    service.handle_reduce_completed(event)

    assert task_repository.completed == [event]


def test_reduce_completion_ignores_unknown_job_or_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = JobPlanState(bucket="bucket-1", map_task_ids={"map-1"}, reduce_task_ids={"reduce-0"})
    service = PlannerService({"job-1": state})
    finalizations = []
    monkeypatch.setattr(planner_service, "finalize_job", lambda job_id, bucket: finalizations.append((job_id, bucket)))

    service.handle_reduce_completed(make_completed_event("reduce-0", TaskType.REDUCE, job_id="missing-job"))
    service.handle_reduce_completed(make_completed_event("unknown-reduce", TaskType.REDUCE))

    assert state.completed_reduce_task_ids == set()
    assert state.done is False
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


def test_handle_task_completed_ignores_done_in_memory_job() -> None:
    state = JobPlanState(bucket="bucket-1", map_task_ids={"map-1"}, done=True)
    service = PlannerService({"job-1": state})
    calls = []
    service.handle_map_completed = lambda ch, event: calls.append(event)  # type: ignore[method-assign]

    service.handle_task_completed(object(), make_completed_event("map-1", TaskType.MAP))

    assert calls == []


def test_handle_task_completed_ignores_failed_persisted_job() -> None:
    job_repository = RecordingJobRepository({"job-1": {"job_id": "job-1", "status": "failed"}})
    state = JobPlanState(bucket="bucket-1", map_task_ids={"map-1"})
    service = PlannerService({"job-1": state}, job_repository=job_repository)
    calls = []
    service.handle_map_completed = lambda ch, event: calls.append(event)  # type: ignore[method-assign]

    service.handle_task_completed(object(), make_completed_event("map-1", TaskType.MAP))

    assert calls == []


def test_handle_task_dead_records_failed_task_and_job(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task_repository = RecordingTaskRepository()
    job_repository = RecordingJobRepository()
    state = JobPlanState(bucket="bucket-1", map_task_ids={"map-1"})
    service = PlannerService(
        {"job-1": state},
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
    assert state.done is True


def test_fail_timed_out_tasks_marks_task_and_job_failed() -> None:
    task_repository = RecordingTaskRepository()
    job_repository = RecordingJobRepository()
    state = JobPlanState(bucket="bucket-1", map_task_ids={"map-1"})
    service = PlannerService(
        {"job-1": state},
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
    assert state.done is True


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

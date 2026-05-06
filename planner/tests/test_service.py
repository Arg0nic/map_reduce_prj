from types import SimpleNamespace

import pytest

import planner.service as planner_service
from libs.models import JobUploadedEvent, TaskCompletedEvent, TaskType
from planner.service import PlannerService
from planner.state import JobPlanState


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

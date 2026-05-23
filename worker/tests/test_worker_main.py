import json
from types import SimpleNamespace

import pytest

import worker.cancellation as cancellation
import worker.main as worker_main
from libs.models import TaskType


class FakeChannel:
    '''
        Minimal RabbitMQ channel fake for callback tests.
    '''

    def __init__(self):
        self.published = []
        self.acked = []

    def basic_publish(self, exchange, routing_key, body, properties=None):
        self.published.append(
            {
                "exchange": exchange,
                "routing_key": routing_key,
                "body": body,
                "properties": properties,
            }
        )

    def basic_ack(self, delivery_tag):
        self.acked.append(delivery_tag)


def make_method(delivery_tag: str = "delivery-1"):
    return SimpleNamespace(delivery_tag=delivery_tag)


def make_properties(headers: dict | None = None):
    return SimpleNamespace(headers=headers)


@pytest.fixture(autouse=True)
def clear_cancelled_jobs() -> None:
    cancellation.clear_cancelled_jobs()


def test_current_task_snapshot_returns_running_task(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(worker_main, "DEFAULT_BUCKET", "bucket-default")
    task = {
        "job_id": "job-1",
        "task_id": "map-2",
        "type": "map",
    }

    worker_main.set_current_task(task, TaskType.MAP, started_at=123.45)

    assert worker_main.get_current_task_snapshot() == {
        "job_id": "job-1",
        "task_id": "map-2",
        "type": "map",
        "bucket": "bucket-default",
        "started_at": 123.45,
        "part_num": None,
    }

    worker_main.clear_current_task("map-2")
    assert worker_main.get_current_task_snapshot() is None


def test_publish_task_completed_publishes_planner_event(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(worker_main, "WORKER_ID", "worker-1")
    monkeypatch.setattr(worker_main.time, "time", lambda: 123.45)
    channel = FakeChannel()

    worker_main.publish_task_completed(
        channel,
        {
            "job_id": "job-1",
            "task_id": "reduce-2",
            "bucket": "bucket-1",
            "part_num": 2,
        },
        TaskType.REDUCE,
    )

    assert len(channel.published) == 1
    published = channel.published[0]
    body = json.loads(published["body"])
    assert published["routing_key"] == worker_main.TASK_COMPLETED_QUEUE
    assert published["properties"].delivery_mode == 2
    assert published["properties"].content_type == "application/json"
    assert body == {
        "job_id": "job-1",
        "task_id": "reduce-2",
        "task_type": "reduce",
        "worker_id": "worker-1",
        "bucket": "bucket-1",
        "completed_at": 123.45,
        "part_num": 2,
    }


def test_callback_acks_invalid_json_without_requeue() -> None:
    channel = FakeChannel()

    worker_main.callback(
        channel,
        make_method(),
        make_properties(),
        body=b"not json",
    )

    assert channel.acked == ["delivery-1"]
    assert channel.published == []


def test_callback_skips_cancelled_task_before_processing(monkeypatch: pytest.MonkeyPatch) -> None:
    task = {
        "job_id": "job-1",
        "task_id": "map-1",
        "type": "map",
        "bucket": "bucket-1",
    }
    cancellation.mark_job_cancelled("job-1")
    channel = FakeChannel()
    calls = []
    monkeypatch.setattr(worker_main, "build_task_paths", lambda *args: calls.append("paths"))
    monkeypatch.setattr(worker_main, "process_map_task", lambda *args, **kwargs: calls.append("process"))

    worker_main.callback(
        channel,
        make_method(),
        make_properties(),
        body=json.dumps(task),
    )

    assert calls == []
    assert channel.acked == ["delivery-1"]
    assert channel.published == []


@pytest.mark.parametrize(
    ("task_type", "processor_name"),
    [
        ("map", "process_map_task"),
        ("reduce", "process_reduce_task"),
    ],
)
def test_callback_processes_task_publishes_completion_and_acks(
    monkeypatch: pytest.MonkeyPatch,
    task_type: str,
    processor_name: str,
) -> None:
    monkeypatch.setattr(worker_main, "WORKER_ID", "worker-1")
    task_paths = object()
    calls = []
    task = {
        "job_id": "job-1",
        "task_id": f"{task_type}-1",
        "type": task_type,
        "bucket": "bucket-1",
    }
    channel = FakeChannel()

    monkeypatch.setattr(
        worker_main,
        "build_task_paths",
        lambda job_id, task_id: calls.append(("paths", job_id, task_id)) or task_paths,
    )
    monkeypatch.setattr(
        worker_main,
        processor_name,
        lambda received_task, received_paths, worker_id: calls.append(
            ("process", received_task, received_paths, worker_id)
        ),
    )
    monkeypatch.setattr(
        worker_main,
        "publish_task_completed",
        lambda ch, received_task, received_task_type: calls.append(
            ("completed", ch, received_task, received_task_type)
        ),
    )

    worker_main.callback(
        channel,
        make_method(),
        make_properties(),
        body=json.dumps(task),
    )

    assert calls == [
        ("paths", "job-1", f"{task_type}-1"),
        ("process", task, task_paths, "worker-1"),
        ("completed", channel, task, TaskType(task_type)),
    ]
    assert channel.acked == ["delivery-1"]
    assert channel.published == []
    assert worker_main.get_current_task_snapshot() is None


def test_callback_does_not_publish_completion_when_job_cancelled_after_processing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(worker_main, "WORKER_ID", "worker-1")
    task_paths = object()
    calls = []
    task = {
        "job_id": "job-1",
        "task_id": "map-1",
        "type": "map",
        "bucket": "bucket-1",
    }
    channel = FakeChannel()

    def fake_process(received_task, received_paths, worker_id):
        calls.append(("process", received_task, received_paths, worker_id))
        cancellation.mark_job_cancelled("job-1")

    monkeypatch.setattr(worker_main, "build_task_paths", lambda job_id, task_id: task_paths)
    monkeypatch.setattr(worker_main, "process_map_task", fake_process)
    monkeypatch.setattr(
        worker_main,
        "publish_task_completed",
        lambda ch, received_task, received_task_type: calls.append("completed"),
    )

    worker_main.callback(
        channel,
        make_method(),
        make_properties(),
        body=json.dumps(task),
    )

    assert calls == [("process", task, task_paths, "worker-1")]
    assert channel.acked == ["delivery-1"]
    assert channel.published == []
    assert worker_main.get_current_task_snapshot() is None


def test_callback_requeues_failed_task_with_incremented_attempts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(worker_main, "WORKER_ID", "worker-1")
    monkeypatch.setattr(worker_main, "process_map_task", lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("bad")))
    monkeypatch.setattr(worker_main.time, "time", lambda: 123.45)
    channel = FakeChannel()
    body = json.dumps({"job_id": "job-1", "task_id": "map-1", "type": "map"})

    worker_main.callback(
        channel,
        make_method(),
        make_properties(headers={"x-attempts": 0}),
        body=body,
    )

    assert channel.acked == ["delivery-1"]
    assert len(channel.published) == 1
    published = channel.published[0]
    assert published["routing_key"] == worker_main.QUEUE_NAME
    assert published["body"] == body
    assert published["properties"].headers["x-attempts"] == 1
    assert published["properties"].delivery_mode == 2


def test_callback_acks_failed_cancelled_task_without_requeue(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(worker_main, "WORKER_ID", "worker-1")

    def fake_process(*args, **kwargs):
        cancellation.mark_job_cancelled("job-1")
        raise ValueError("bad")

    monkeypatch.setattr(worker_main, "process_map_task", fake_process)
    channel = FakeChannel()
    body = json.dumps({"job_id": "job-1", "task_id": "map-1", "type": "map"})

    worker_main.callback(
        channel,
        make_method(),
        make_properties(headers={"x-attempts": 0}),
        body=body,
    )

    assert channel.acked == ["delivery-1"]
    assert channel.published == []


def test_callback_sends_failed_task_to_dead_queue_after_max_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(worker_main, "WORKER_ID", "worker-1")
    monkeypatch.setattr(worker_main, "process_map_task", lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("bad")))
    monkeypatch.setattr(worker_main.time, "time", lambda: 123.45)
    channel = FakeChannel()
    body = json.dumps({"job_id": "job-1", "task_id": "map-1", "type": "map"})

    worker_main.callback(
        channel,
        make_method(),
        make_properties(headers={"x-attempts": worker_main.MAX_RETRIES - 1}),
        body=body,
    )

    assert channel.acked == ["delivery-1"]
    assert len(channel.published) == 1
    published = channel.published[0]
    assert published["routing_key"] == worker_main.DEAD_QUEUE_NAME
    assert published["body"] == body
    assert published["properties"].headers["x-attempts"] == worker_main.MAX_RETRIES
    assert published["properties"].delivery_mode == 2


def test_callback_requeues_unknown_task_type() -> None:
    channel = FakeChannel()
    body = json.dumps({"job_id": "job-1", "task_id": "bad-1", "type": "bad"})

    worker_main.callback(
        channel,
        make_method(),
        make_properties(headers={}),
        body=body,
    )

    assert channel.acked == ["delivery-1"]
    assert len(channel.published) == 1
    assert channel.published[0]["routing_key"] == worker_main.QUEUE_NAME
    assert channel.published[0]["properties"].headers["x-attempts"] == 1

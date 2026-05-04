import json
from types import SimpleNamespace

import pytest

import worker.main as worker_main


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
        "reduce",
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
        ("completed", channel, task, task_type),
    ]
    assert channel.acked == ["delivery-1"]
    assert channel.published == []


def test_callback_requeues_failed_task_with_incremented_attempts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(worker_main, "WORKER_ID", "worker-1")
    monkeypatch.setattr(worker_main, "process_map_task", lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("bad")))
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


def test_callback_sends_failed_task_to_dead_queue_after_max_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(worker_main, "WORKER_ID", "worker-1")
    monkeypatch.setattr(worker_main, "process_map_task", lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("bad")))
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

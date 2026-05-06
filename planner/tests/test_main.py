import json
from types import SimpleNamespace

import pytest

import planner.main as planner_main


class FakeChannel:
    '''
        Minimal RabbitMQ channel fake for planner callback tests.
    '''

    def __init__(self):
        self.acked = []
        self.nacked = []

    def basic_ack(self, delivery_tag):
        self.acked.append(delivery_tag)

    def basic_nack(self, delivery_tag, requeue):
        self.nacked.append((delivery_tag, requeue))


def make_method(delivery_tag: str = "delivery-1"):
    return SimpleNamespace(delivery_tag=delivery_tag)


def test_heartbeat_callback_acks_valid_heartbeat() -> None:
    channel = FakeChannel()

    planner_main.heartbeat_callback(
        channel,
        make_method(),
        properties=None,
        body=json.dumps({"worker_id": "worker-1", "ts": 123.45}),
    )

    assert channel.acked == ["delivery-1"]
    assert channel.nacked == []


def test_heartbeat_callback_acks_invalid_json() -> None:
    channel = FakeChannel()

    planner_main.heartbeat_callback(channel, make_method(), properties=None, body=b"not-json")

    assert channel.acked == ["delivery-1"]
    assert channel.nacked == []


def test_job_callback_delegates_valid_job_event_and_acks(monkeypatch: pytest.MonkeyPatch) -> None:
    channel = FakeChannel()
    calls = []

    class FakePlannerService:
        def handle_job_uploaded(self, ch, event):
            calls.append((ch, event.job_id, event.bucket, event.chunks_prefix))

    monkeypatch.setattr(planner_main, "PLANNER_SERVICE", FakePlannerService())

    planner_main.job_callback(
        channel,
        make_method(),
        properties=None,
        body=json.dumps(
            {
                "job_id": "job-1",
                "bucket": "bucket-1",
                "chunks_prefix": "jobs/job-1/chunks/",
                "created_at": 123.45,
            }
        ),
    )

    assert calls == [(channel, "job-1", "bucket-1", "jobs/job-1/chunks/")]
    assert channel.acked == ["delivery-1"]
    assert channel.nacked == []


def test_job_callback_acks_invalid_job_event() -> None:
    channel = FakeChannel()

    planner_main.job_callback(channel, make_method(), properties=None, body=b"not-json")

    assert channel.acked == ["delivery-1"]
    assert channel.nacked == []


def test_job_callback_nacks_when_planning_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    channel = FakeChannel()

    class FailingPlannerService:
        def handle_job_uploaded(self, ch, event):
            raise RuntimeError("planning failed")

    monkeypatch.setattr(planner_main, "PLANNER_SERVICE", FailingPlannerService())

    planner_main.job_callback(
        channel,
        make_method(),
        properties=None,
        body=json.dumps(
            {
                "job_id": "job-1",
                "bucket": "bucket-1",
                "chunks_prefix": "jobs/job-1/chunks/",
                "created_at": 123.45,
            }
        ),
    )

    assert channel.acked == []
    assert channel.nacked == [("delivery-1", True)]


def test_task_completed_callback_delegates_valid_event_and_acks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    channel = FakeChannel()
    calls = []

    class FakePlannerService:
        def handle_task_completed(self, ch, event):
            calls.append((ch, event.job_id, event.task_id, event.task_type))

    monkeypatch.setattr(planner_main, "PLANNER_SERVICE", FakePlannerService())

    planner_main.task_completed_callback(
        channel,
        make_method(),
        properties=None,
        body=json.dumps(
            {
                "job_id": "job-1",
                "task_id": "map-1",
                "task_type": "map",
                "worker_id": "worker-1",
                "bucket": "bucket-1",
                "completed_at": 123.45,
            }
        ),
    )

    assert calls == [(channel, "job-1", "map-1", "map")]
    assert channel.acked == ["delivery-1"]
    assert channel.nacked == []


def test_task_completed_callback_acks_invalid_event() -> None:
    channel = FakeChannel()

    planner_main.task_completed_callback(channel, make_method(), properties=None, body=b"not-json")

    assert channel.acked == ["delivery-1"]
    assert channel.nacked == []


def test_task_completed_callback_nacks_when_handling_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    channel = FakeChannel()

    class FailingPlannerService:
        def handle_task_completed(self, ch, event):
            raise RuntimeError("handling failed")

    monkeypatch.setattr(planner_main, "PLANNER_SERVICE", FailingPlannerService())

    planner_main.task_completed_callback(
        channel,
        make_method(),
        properties=None,
        body=json.dumps(
            {
                "job_id": "job-1",
                "task_id": "reduce-1",
                "task_type": "reduce",
                "worker_id": "worker-1",
                "bucket": "bucket-1",
                "completed_at": 123.45,
                "part_num": 1,
            }
        ),
    )

    assert channel.acked == []
    assert channel.nacked == [("delivery-1", True)]

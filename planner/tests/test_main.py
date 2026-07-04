import json
from types import SimpleNamespace

import pytest

from libs.heartbeat_queue import HEARTBEAT_QUEUE, HEARTBEAT_QUEUE_ARGUMENTS
from libs.models import WorkerHeartbeat
import planner.main as planner_main


class FakeChannel:
    '''
        Minimal RabbitMQ channel fake for planner callback tests.
    '''

    def __init__(self):
        self.acked = []
        self.nacked = []
        self.deleted = []
        self.declared = []
        self.purged = []

    def basic_ack(self, delivery_tag):
        self.acked.append(delivery_tag)

    def basic_nack(self, delivery_tag, requeue):
        self.nacked.append((delivery_tag, requeue))

    def queue_delete(self, queue):
        self.deleted.append(queue)

    def queue_declare(self, queue, durable, arguments=None):
        self.declared.append((queue, durable, arguments))

    def queue_purge(self, queue):
        self.purged.append(queue)


def make_method(delivery_tag: str = "delivery-1"):
    return SimpleNamespace(delivery_tag=delivery_tag)


def test_prepare_heartbeat_queue_declares_ttl_queue_and_purges_stale_messages() -> None:
    channel = FakeChannel()
    connection = SimpleNamespace(channel=lambda: channel)

    prepared_channel = planner_main.prepare_heartbeat_queue(connection, channel)

    assert prepared_channel is channel
    assert channel.declared == [(HEARTBEAT_QUEUE, False, HEARTBEAT_QUEUE_ARGUMENTS)]
    assert channel.purged == [HEARTBEAT_QUEUE]


def test_prepare_heartbeat_queue_recreates_queue_with_incompatible_arguments() -> None:
    class IncompatibleHeartbeatChannel(FakeChannel):
        def queue_declare(self, queue, durable, arguments=None):
            raise planner_main.ChannelClosedByBroker(406, "PRECONDITION_FAILED")

    old_channel = IncompatibleHeartbeatChannel()
    new_channel = FakeChannel()
    connection = SimpleNamespace(channel=lambda: new_channel)

    prepared_channel = planner_main.prepare_heartbeat_queue(connection, old_channel)

    assert prepared_channel is new_channel
    assert new_channel.deleted == [HEARTBEAT_QUEUE]
    assert new_channel.declared == [(HEARTBEAT_QUEUE, False, HEARTBEAT_QUEUE_ARGUMENTS)]
    assert new_channel.purged == [HEARTBEAT_QUEUE]


def test_heartbeat_callback_delegates_idle_heartbeat_and_acks(monkeypatch: pytest.MonkeyPatch) -> None:
    channel = FakeChannel()
    calls = []

    class FakePlannerService:
        def handle_worker_heartbeat(self, heartbeat):
            calls.append(heartbeat)

    monkeypatch.setattr(planner_main, "PLANNER_SERVICE", FakePlannerService())
    heartbeat = {"worker_id": "worker-1", "ts": 123.45}

    planner_main.heartbeat_callback(
        channel,
        make_method(),
        properties=None,
        body=json.dumps(heartbeat),
    )

    assert calls == [WorkerHeartbeat.model_validate(heartbeat)]
    assert channel.acked == ["delivery-1"]
    assert channel.nacked == []


def test_heartbeat_callback_delegates_current_task_and_acks(monkeypatch: pytest.MonkeyPatch) -> None:
    channel = FakeChannel()
    calls = []

    class FakePlannerService:
        def handle_worker_heartbeat(self, heartbeat):
            calls.append(heartbeat)

    monkeypatch.setattr(planner_main, "PLANNER_SERVICE", FakePlannerService())
    heartbeat = {
        "worker_id": "worker-1",
        "ts": 123.45,
        "current_task": {
            "job_id": "job-1",
            "task_id": "map-1",
            "type": "map",
            "started_at": 120.0,
        },
    }

    planner_main.heartbeat_callback(
        channel,
        make_method(),
        properties=None,
        body=json.dumps(heartbeat),
    )

    assert calls == [WorkerHeartbeat.model_validate(heartbeat)]
    assert channel.acked == ["delivery-1"]
    assert channel.nacked == []


def test_heartbeat_callback_acks_when_heartbeat_handling_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    channel = FakeChannel()

    class FailingPlannerService:
        def handle_worker_heartbeat(self, heartbeat):
            raise RuntimeError("handling failed")

    monkeypatch.setattr(planner_main, "PLANNER_SERVICE", FailingPlannerService())

    planner_main.heartbeat_callback(
        channel,
        make_method(),
        properties=None,
        body=json.dumps(
            {
                "worker_id": "worker-1",
                "ts": 123.45,
                "current_task": {
                    "job_id": "job-1",
                    "task_id": "map-1",
                    "type": "map",
                    "started_at": 120.0,
                },
            }
        ),
    )

    assert channel.acked == ["delivery-1"]
    assert channel.nacked == []


def test_heartbeat_callback_acks_invalid_json() -> None:
    channel = FakeChannel()

    planner_main.heartbeat_callback(channel, make_method(), properties=None, body=b"not-json")

    assert channel.acked == ["delivery-1"]
    assert channel.nacked == []


def test_heartbeat_callback_acks_invalid_heartbeat_payload() -> None:
    channel = FakeChannel()

    planner_main.heartbeat_callback(
        channel,
        make_method(),
        properties=None,
        body=json.dumps({"worker_id": "worker-1"}),
    )

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


def test_task_dead_callback_delegates_valid_dead_task_and_acks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    channel = FakeChannel()
    calls = []

    class FakePlannerService:
        def handle_task_dead(self, task):
            calls.append(task)

    monkeypatch.setattr(planner_main, "PLANNER_SERVICE", FakePlannerService())

    planner_main.task_dead_callback(
        channel,
        make_method(),
        properties=None,
        body=json.dumps(
            {
                "job_id": "job-1",
                "task_id": "map-1",
                "type": "map",
                "address": "jobs/job-1/chunks/chunk_0.txt",
                "storage": "minio",
                "bucket": "bucket-1",
                "created_at": 123.45,
            }
        ),
    )

    assert calls == [
        {
            "job_id": "job-1",
            "task_id": "map-1",
            "type": "map",
            "address": "jobs/job-1/chunks/chunk_0.txt",
            "storage": "minio",
            "bucket": "bucket-1",
            "created_at": 123.45,
            "part_num": None,
        }
    ]
    assert channel.acked == ["delivery-1"]
    assert channel.nacked == []


def test_task_dead_callback_acks_invalid_dead_task() -> None:
    channel = FakeChannel()

    planner_main.task_dead_callback(channel, make_method(), properties=None, body=b"not-json")

    assert channel.acked == ["delivery-1"]
    assert channel.nacked == []


def test_task_dead_callback_nacks_when_handling_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    channel = FakeChannel()

    class FailingPlannerService:
        def handle_task_dead(self, task):
            raise RuntimeError("handling failed")

    monkeypatch.setattr(planner_main, "PLANNER_SERVICE", FailingPlannerService())

    planner_main.task_dead_callback(
        channel,
        make_method(),
        properties=None,
        body=json.dumps(
            {
                "job_id": "job-1",
                "task_id": "reduce-1",
                "type": "reduce",
                "address": "jobs/job-1/shuffle/part_1/",
                "storage": "minio",
                "bucket": "bucket-1",
                "created_at": 123.45,
                "part_num": 1,
            }
        ),
    )

    assert channel.acked == []
    assert channel.nacked == [("delivery-1", True)]

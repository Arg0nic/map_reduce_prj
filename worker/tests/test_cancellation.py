import json
from types import SimpleNamespace

import pytest

import worker.cancellation as cancellation


@pytest.fixture(autouse=True)
def clear_cancelled_jobs() -> None:
    cancellation.clear_cancelled_jobs()


def test_mark_job_cancelled_records_job_id() -> None:
    assert cancellation.is_job_cancelled("job-1") is False

    cancellation.mark_job_cancelled("job-1")

    assert cancellation.is_job_cancelled("job-1") is True
    assert cancellation.is_job_cancelled("job-2") is False
    assert cancellation.is_job_cancelled(None) is False


def test_start_cancellation_listener_thread_starts_daemon_thread(monkeypatch: pytest.MonkeyPatch) -> None:
    created_threads = []

    class FakeThread:
        def __init__(self, target, kwargs, daemon):
            self.target = target
            self.kwargs = kwargs
            self.daemon = daemon
            self.started = False
            created_threads.append(self)

        def start(self) -> None:
            self.started = True

    monkeypatch.setattr(cancellation.threading, "Thread", FakeThread)

    thread = cancellation.start_cancellation_listener_thread(
        worker_id="worker-1",
        rabbit_login="login",
        rabbit_pass="pass",
        rabbit_host="localhost",
        rabbit_port=5672,
    )

    assert thread is created_threads[0]
    assert thread.target is cancellation.cancellation_loop
    assert thread.kwargs == {
        "worker_id": "worker-1",
        "rabbit_login": "login",
        "rabbit_pass": "pass",
        "rabbit_host": "localhost",
        "rabbit_port": 5672,
    }
    assert thread.daemon is True
    assert thread.started is True


def test_cancellation_loop_consumes_fanout_event(monkeypatch: pytest.MonkeyPatch) -> None:
    class StopLoop(Exception):
        pass

    created = {}

    class FakeCredentials:
        def __init__(self, login, password):
            self.login = login
            self.password = password

    class FakeConnectionParameters:
        def __init__(self, host, port, virtual_host, credentials):
            self.host = host
            self.port = port
            self.virtual_host = virtual_host
            self.credentials = credentials

    class FakeChannel:
        def __init__(self):
            self.exchanges = []
            self.binds = []
            self.acked = []
            self.callback = None

        def exchange_declare(self, exchange, exchange_type, durable):
            self.exchanges.append((exchange, exchange_type, durable))

        def queue_declare(self, queue, exclusive):
            return SimpleNamespace(method=SimpleNamespace(queue="worker-cancel-queue"))

        def queue_bind(self, exchange, queue):
            self.binds.append((exchange, queue))

        def basic_consume(self, queue, on_message_callback, auto_ack):
            self.callback = on_message_callback
            self.consume_args = (queue, auto_ack)

        def basic_ack(self, delivery_tag):
            self.acked.append(delivery_tag)

        def start_consuming(self):
            self.callback(
                self,
                SimpleNamespace(delivery_tag="delivery-1"),
                None,
                json.dumps(
                    {
                        "job_id": "job-1",
                        "reason": "failed",
                        "cancelled_at": 123.45,
                    }
                ),
            )
            raise StopLoop()

    class FakeConnection:
        def __init__(self, params):
            self.params = params
            self.channel_obj = FakeChannel()
            created["connection"] = self

        def channel(self):
            return self.channel_obj

    monkeypatch.setattr(cancellation.pika, "PlainCredentials", FakeCredentials)
    monkeypatch.setattr(cancellation.pika, "ConnectionParameters", FakeConnectionParameters)
    monkeypatch.setattr(cancellation.pika, "BlockingConnection", FakeConnection)

    with pytest.raises(StopLoop):
        cancellation.cancellation_loop(
            worker_id="worker-1",
            rabbit_login="login",
            rabbit_pass="pass",
            rabbit_host="localhost",
            rabbit_port=5672,
        )

    channel = created["connection"].channel_obj
    assert channel.exchanges == [(cancellation.JOB_CANCELLED_EXCHANGE, "fanout", True)]
    assert channel.binds == [(cancellation.JOB_CANCELLED_EXCHANGE, "worker-cancel-queue")]
    assert channel.consume_args == ("worker-cancel-queue", False)
    assert channel.acked == ["delivery-1"]
    assert cancellation.is_job_cancelled("job-1") is True


def test_cancellation_loop_acks_invalid_event(monkeypatch: pytest.MonkeyPatch) -> None:
    class StopLoop(Exception):
        pass

    created = {}

    class FakeCredentials:
        def __init__(self, login, password):
            self.login = login
            self.password = password

    class FakeConnectionParameters:
        def __init__(self, host, port, virtual_host, credentials):
            pass

    class FakeChannel:
        def __init__(self):
            self.acked = []
            self.callback = None

        def exchange_declare(self, exchange, exchange_type, durable):
            pass

        def queue_declare(self, queue, exclusive):
            return SimpleNamespace(method=SimpleNamespace(queue="worker-cancel-queue"))

        def queue_bind(self, exchange, queue):
            pass

        def basic_consume(self, queue, on_message_callback, auto_ack):
            self.callback = on_message_callback

        def basic_ack(self, delivery_tag):
            self.acked.append(delivery_tag)

        def start_consuming(self):
            self.callback(self, SimpleNamespace(delivery_tag="delivery-1"), None, b"not-json")
            raise StopLoop()

    class FakeConnection:
        def __init__(self, params):
            self.channel_obj = FakeChannel()
            created["connection"] = self

        def channel(self):
            return self.channel_obj

    monkeypatch.setattr(cancellation.pika, "PlainCredentials", FakeCredentials)
    monkeypatch.setattr(cancellation.pika, "ConnectionParameters", FakeConnectionParameters)
    monkeypatch.setattr(cancellation.pika, "BlockingConnection", FakeConnection)

    with pytest.raises(StopLoop):
        cancellation.cancellation_loop(
            worker_id="worker-1",
            rabbit_login="login",
            rabbit_pass="pass",
            rabbit_host="localhost",
            rabbit_port=5672,
        )

    assert created["connection"].channel_obj.acked == ["delivery-1"]
    assert cancellation.is_job_cancelled("job-1") is False

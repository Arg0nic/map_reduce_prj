import json

import pytest

import worker.heartbeat as heartbeat


def test_start_heartbeat_thread_starts_daemon_thread(monkeypatch: pytest.MonkeyPatch) -> None:
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

    monkeypatch.setattr(heartbeat.threading, "Thread", FakeThread)

    thread = heartbeat.start_heartbeat_thread(
        worker_id="worker-1",
        rabbit_login="login",
        rabbit_pass="pass",
        rabbit_host="localhost",
        rabbit_port=5672,
        interval_seconds=10,
    )

    assert thread is created_threads[0]
    assert thread.target is heartbeat.heartbeat_loop
    assert thread.kwargs == {
        "worker_id": "worker-1",
        "rabbit_login": "login",
        "rabbit_pass": "pass",
        "rabbit_host": "localhost",
        "rabbit_port": 5672,
        "interval_seconds": 10,
        "task_snapshot_provider": None,
    }
    assert thread.daemon is True
    assert thread.started is True


def test_heartbeat_loop_publishes_heartbeat_message(monkeypatch: pytest.MonkeyPatch) -> None:
    class StopLoop(Exception):
        pass

    created = {}

    class FakeCredentials:
        def __init__(self, login, password):
            self.login = login
            self.password = password

    class FakeConnectionParameters:
        def __init__(self, host, port, credentials):
            self.host = host
            self.port = port
            self.credentials = credentials

    class FakeChannel:
        def __init__(self):
            self.declared = []
            self.published = []

        def queue_declare(self, queue, durable):
            self.declared.append((queue, durable))

        def basic_publish(self, exchange, routing_key, body):
            self.published.append((exchange, routing_key, body))

    class FakeConnection:
        def __init__(self, params):
            self.params = params
            self.channel_obj = FakeChannel()
            created["connection"] = self

        def channel(self):
            return self.channel_obj

    monkeypatch.setattr(heartbeat.pika, "PlainCredentials", FakeCredentials)
    monkeypatch.setattr(heartbeat.pika, "ConnectionParameters", FakeConnectionParameters)
    monkeypatch.setattr(heartbeat.pika, "BlockingConnection", FakeConnection)
    monkeypatch.setattr(heartbeat.time, "time", lambda: 123.45)
    monkeypatch.setattr(heartbeat.time, "sleep", lambda interval: (_ for _ in ()).throw(StopLoop()))

    with pytest.raises(StopLoop):
        heartbeat.heartbeat_loop(
            worker_id="worker-1",
            rabbit_login="login",
            rabbit_pass="pass",
            rabbit_host="localhost",
            rabbit_port=5672,
            interval_seconds=10,
        )

    connection = created["connection"]
    channel = connection.channel_obj
    assert connection.params.host == "localhost"
    assert connection.params.port == 5672
    assert connection.params.credentials.login == "login"
    assert connection.params.credentials.password == "pass"
    assert channel.declared == []
    assert len(channel.published) == 1
    exchange, routing_key, body = channel.published[0]
    assert exchange == ""
    assert routing_key == heartbeat.HEARTBEAT_QUEUE
    assert json.loads(body) == {
        "worker_id": "worker-1",
        "ts": 123.45,
    }


def test_heartbeat_loop_includes_current_task(monkeypatch: pytest.MonkeyPatch) -> None:
    class StopLoop(Exception):
        pass

    created = {}

    class FakeCredentials:
        def __init__(self, login, password):
            self.login = login
            self.password = password

    class FakeConnectionParameters:
        def __init__(self, host, port, credentials):
            self.host = host
            self.port = port
            self.credentials = credentials

    class FakeChannel:
        def __init__(self):
            self.published = []

        def queue_declare(self, queue, durable):
            pass

        def basic_publish(self, exchange, routing_key, body):
            self.published.append((exchange, routing_key, body))

    class FakeConnection:
        def __init__(self, params):
            self.channel_obj = FakeChannel()
            created["connection"] = self

        def channel(self):
            return self.channel_obj

    monkeypatch.setattr(heartbeat.pika, "PlainCredentials", FakeCredentials)
    monkeypatch.setattr(heartbeat.pika, "ConnectionParameters", FakeConnectionParameters)
    monkeypatch.setattr(heartbeat.pika, "BlockingConnection", FakeConnection)
    monkeypatch.setattr(heartbeat.time, "time", lambda: 123.45)
    monkeypatch.setattr(heartbeat.time, "sleep", lambda interval: (_ for _ in ()).throw(StopLoop()))

    current_task = {
        "job_id": "job-1",
        "task_id": "map-1",
        "type": "map",
        "started_at": 120.0,
    }

    with pytest.raises(StopLoop):
        heartbeat.heartbeat_loop(
            worker_id="worker-1",
            rabbit_login="login",
            rabbit_pass="pass",
            rabbit_host="localhost",
            rabbit_port=5672,
            interval_seconds=10,
            task_snapshot_provider=lambda: current_task,
        )

    body = created["connection"].channel_obj.published[0][2]
    assert json.loads(body) == {
        "worker_id": "worker-1",
        "ts": 123.45,
        "current_task": {
            **current_task,
            "bucket": "mapreduce-data",
        },
    }

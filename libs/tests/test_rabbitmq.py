import pytest

from libs import rabbitmq


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


class FakeConnection:
    def __init__(self, params):
        self.params = params


def test_create_blocking_connection_connects_with_expected_params(monkeypatch: pytest.MonkeyPatch) -> None:
    created = {}

    def fake_blocking_connection(params):
        connection = FakeConnection(params)
        created["connection"] = connection
        return connection

    monkeypatch.setattr(rabbitmq.pika, "PlainCredentials", FakeCredentials)
    monkeypatch.setattr(rabbitmq.pika, "ConnectionParameters", FakeConnectionParameters)
    monkeypatch.setattr(rabbitmq.pika, "BlockingConnection", fake_blocking_connection)

    connection = rabbitmq.create_blocking_connection(
        rabbit_login="login",
        rabbit_pass="pass",
        rabbit_host="rabbitmq",
        rabbit_port=5672,
        service_name="Test",
    )

    assert connection is created["connection"]
    assert connection.params.host == "rabbitmq"
    assert connection.params.port == 5672
    assert connection.params.virtual_host == "/"
    assert connection.params.credentials.login == "login"
    assert connection.params.credentials.password == "pass"


def test_create_blocking_connection_retries_until_rabbitmq_is_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts = []
    sleeps = []

    def fake_blocking_connection(params):
        attempts.append(params)
        if len(attempts) < 3:
            raise rabbitmq.pika.exceptions.AMQPConnectionError()
        return FakeConnection(params)

    monkeypatch.setattr(rabbitmq.pika, "PlainCredentials", FakeCredentials)
    monkeypatch.setattr(rabbitmq.pika, "ConnectionParameters", FakeConnectionParameters)
    monkeypatch.setattr(rabbitmq.pika, "BlockingConnection", fake_blocking_connection)
    monkeypatch.setattr(rabbitmq.time, "sleep", lambda seconds: sleeps.append(seconds))

    connection = rabbitmq.create_blocking_connection(
        rabbit_login="login",
        rabbit_pass="pass",
        rabbit_host="rabbitmq",
        rabbit_port=5672,
        service_name="Test",
        retries=3,
        retry_delay_seconds=0.5,
    )

    assert isinstance(connection, FakeConnection)
    assert len(attempts) == 3
    assert sleeps == [0.5, 0.5]

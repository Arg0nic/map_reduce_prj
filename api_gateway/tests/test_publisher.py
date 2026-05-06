import json

import pytest

import api_gateway.publisher as publisher
from libs.models import JobUploadedEvent


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
    def __init__(self, fail_publish: bool = False):
        self.fail_publish = fail_publish
        self.declared = []
        self.published = []

    def queue_declare(self, queue, durable):
        self.declared.append((queue, durable))

    def basic_publish(self, exchange, routing_key, body, properties):
        if self.fail_publish:
            raise publisher.pika.exceptions.AMQPError("publish failed")
        self.published.append((exchange, routing_key, body, properties))


class FakeConnection:
    def __init__(self, params, fail_publish: bool = False):
        self.params = params
        self.channel_obj = FakeChannel(fail_publish=fail_publish)
        self.closed = False

    def channel(self):
        return self.channel_obj

    def close(self):
        self.closed = True


def test_publish_job_uploaded_publishes_durable_event(monkeypatch: pytest.MonkeyPatch) -> None:
    created = {}

    def fake_blocking_connection(params):
        connection = FakeConnection(params)
        created["connection"] = connection
        return connection

    monkeypatch.setattr(publisher.pika, "PlainCredentials", FakeCredentials)
    monkeypatch.setattr(publisher.pika, "ConnectionParameters", FakeConnectionParameters)
    monkeypatch.setattr(publisher.pika, "BlockingConnection", fake_blocking_connection)
    event = JobUploadedEvent(
        job_id="job-1",
        bucket="bucket-1",
        chunks_prefix="jobs/job-1/chunks/",
        created_at=123.45,
    )
    job_publisher = publisher.RabbitJobEventPublisher(
        queue_name="jobs-test",
        rabbit_login="login",
        rabbit_pass="pass",
        rabbit_host="localhost",
        rabbit_port=5672,
    )

    job_publisher.publish_job_uploaded(event)

    connection = created["connection"]
    channel = connection.channel_obj
    assert connection.params.host == "localhost"
    assert connection.params.port == 5672
    assert connection.params.virtual_host == "/"
    assert connection.params.credentials.login == "login"
    assert connection.params.credentials.password == "pass"
    assert channel.declared == [("jobs-test", True)]
    assert len(channel.published) == 1
    exchange, routing_key, body, properties = channel.published[0]
    assert exchange == ""
    assert routing_key == "jobs-test"
    assert json.loads(body) == {
        "job_id": "job-1",
        "bucket": "bucket-1",
        "chunks_prefix": "jobs/job-1/chunks/",
        "created_at": 123.45,
    }
    assert properties.delivery_mode == 2
    assert properties.content_type == "application/json"
    assert connection.closed is True


def test_publish_job_uploaded_wraps_connection_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(publisher.pika, "PlainCredentials", FakeCredentials)
    monkeypatch.setattr(publisher.pika, "ConnectionParameters", FakeConnectionParameters)
    monkeypatch.setattr(
        publisher.pika,
        "BlockingConnection",
        lambda params: (_ for _ in ()).throw(publisher.pika.exceptions.AMQPError("connect failed")),
    )
    event = JobUploadedEvent(
        job_id="job-1",
        bucket="bucket-1",
        chunks_prefix="jobs/job-1/chunks/",
        created_at=123.45,
    )

    with pytest.raises(RuntimeError, match="Failed to connect to RabbitMQ"):
        publisher.RabbitJobEventPublisher().publish_job_uploaded(event)


def test_publish_job_uploaded_wraps_publish_errors_and_closes_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created = {}

    def fake_blocking_connection(params):
        connection = FakeConnection(params, fail_publish=True)
        created["connection"] = connection
        return connection

    monkeypatch.setattr(publisher.pika, "PlainCredentials", FakeCredentials)
    monkeypatch.setattr(publisher.pika, "ConnectionParameters", FakeConnectionParameters)
    monkeypatch.setattr(publisher.pika, "BlockingConnection", fake_blocking_connection)
    event = JobUploadedEvent(
        job_id="job-1",
        bucket="bucket-1",
        chunks_prefix="jobs/job-1/chunks/",
        created_at=123.45,
    )

    with pytest.raises(RuntimeError, match="Failed to publish job event"):
        publisher.RabbitJobEventPublisher().publish_job_uploaded(event)

    assert created["connection"].closed is True

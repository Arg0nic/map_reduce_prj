import json

from fastapi.testclient import TestClient

import api_gateway.main as api_main
from libs.models import JobStatus


class FakeJobService:
    def __init__(self):
        self.created_job = {"job_id": "job-1"}
        self.create_error = None
        self.jobs = {}

    def create_from_upload(self, file_obj, filename):
        if self.create_error:
            raise self.create_error
        assert filename == "input.txt"
        assert file_obj.read() == b"hello\n"
        return self.created_job

    def get_job(self, job_id: str):
        return self.jobs.get(job_id)


def make_client(service: FakeJobService, monkeypatch):
    monkeypatch.setattr(api_main, "JobService", lambda: service)
    return TestClient(api_main.create_app())


def test_health_endpoint(monkeypatch) -> None:
    client = make_client(FakeJobService(), monkeypatch)

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "service": "api_gateway",
    }


def test_upload_file_returns_job_id(monkeypatch) -> None:
    service = FakeJobService()
    client = make_client(service, monkeypatch)

    response = client.post("/files", files={"file": ("input.txt", b"hello\n", "text/plain")})

    assert response.status_code == 202
    assert response.json() == {"job_id": "job-1"}


def test_upload_file_returns_400_for_empty_input(monkeypatch) -> None:
    service = FakeJobService()
    service.create_error = ValueError("Input file is empty.")
    client = make_client(service, monkeypatch)

    response = client.post("/files", files={"file": ("input.txt", b"hello\n", "text/plain")})

    assert response.status_code == 400
    assert response.json() == {"detail": "Input file is empty."}


def test_upload_file_returns_503_for_publisher_failure(monkeypatch) -> None:
    service = FakeJobService()
    service.create_error = RuntimeError("Failed to connect to RabbitMQ.")
    client = make_client(service, monkeypatch)

    response = client.post("/files", files={"file": ("input.txt", b"hello\n", "text/plain")})

    assert response.status_code == 503
    assert response.json() == {"detail": "Failed to connect to RabbitMQ."}


def test_get_job_result_returns_404_for_unknown_job(monkeypatch) -> None:
    client = make_client(FakeJobService(), monkeypatch)

    response = client.get("/jobs/missing/result")

    assert response.status_code == 404
    assert response.json() == {"detail": "Job not found."}


def test_get_job_result_returns_not_ready_for_unfinished_job(monkeypatch) -> None:
    service = FakeJobService()
    service.jobs["job-1"] = {"job_id": "job-1", "status": JobStatus.UPLOADED.value}
    client = make_client(service, monkeypatch)

    response = client.get("/jobs/job-1/result")

    assert response.status_code == 200
    assert response.json() == {"message": "Not ready yet"}


def test_get_job_result_requires_result_key_for_done_job(monkeypatch) -> None:
    service = FakeJobService()
    service.jobs["job-1"] = {
        "job_id": "job-1",
        "status": JobStatus.DONE.value,
        "bucket": "bucket-1",
    }
    client = make_client(service, monkeypatch)

    response = client.get("/jobs/job-1/result")

    assert response.status_code == 500
    assert response.json() == {"detail": "Job is done but result key is missing."}


def test_get_job_result_reads_result_object(monkeypatch) -> None:
    service = FakeJobService()
    service.jobs["job-1"] = {
        "job_id": "job-1",
        "status": JobStatus.DONE.value,
        "bucket": "bucket-1",
        "result_key": "jobs/job-1/result/result.json",
    }
    reads = []
    monkeypatch.setattr(
        api_main,
        "read_object_bytes",
        lambda bucket, key: reads.append((bucket, key)) or b'{"hello": 2}',
    )
    client = make_client(service, monkeypatch)

    response = client.get("/jobs/job-1/result")

    assert response.status_code == 200
    assert response.json() == {"result": {"hello": 2}}
    assert reads == [("bucket-1", "jobs/job-1/result/result.json")]


def test_get_job_result_returns_500_for_invalid_json(monkeypatch) -> None:
    service = FakeJobService()
    service.jobs["job-1"] = {
        "job_id": "job-1",
        "status": JobStatus.DONE.value,
        "bucket": "bucket-1",
        "result_key": "jobs/job-1/result/result.json",
    }
    monkeypatch.setattr(api_main, "read_object_bytes", lambda bucket, key: b"not-json")
    client = make_client(service, monkeypatch)

    response = client.get("/jobs/job-1/result")

    assert response.status_code == 500
    assert response.json() == {"detail": "Job result is not valid JSON."}


def test_get_job_result_returns_503_for_storage_failure(monkeypatch) -> None:
    service = FakeJobService()
    service.jobs["job-1"] = {
        "job_id": "job-1",
        "status": JobStatus.DONE.value,
        "bucket": "bucket-1",
        "result_key": "jobs/job-1/result/result.json",
    }

    def failing_read(bucket: str, key: str) -> bytes:
        raise RuntimeError("storage unavailable")

    monkeypatch.setattr(api_main, "read_object_bytes", failing_read)
    client = make_client(service, monkeypatch)

    response = client.get("/jobs/job-1/result")

    assert response.status_code == 503
    assert response.json() == {"detail": "Failed to load job result."}

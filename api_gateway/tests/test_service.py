import hashlib
from io import BytesIO

import pytest

import api_gateway.service as api_service
from libs.models import JobStatus


class RecordingRepository:
    '''
        In-memory repository fake for JobService unit tests.
    '''

    def __init__(self):
        self.saved = []
        self.jobs = {}

    def save(self, job: dict) -> dict:
        saved_job = dict(job)
        self.saved.append(saved_job)
        self.jobs[saved_job["job_id"]] = saved_job
        return saved_job

    def load(self, job_id: str) -> dict | None:
        return self.jobs.get(job_id)

    def update(self, job_id: str, patch: dict) -> dict | None:
        job = self.jobs.get(job_id)
        if job is None:
            return None
        job.update(patch)
        return job


class RecordingPublisher:
    def __init__(self):
        self.events = []

    def publish_job_uploaded(self, event) -> None:
        self.events.append(event)


def test_safe_filename_strips_client_paths() -> None:
    assert api_service._get_safe_filename("../../input.txt") == "input.txt"
    assert api_service._get_safe_filename("") == "input.txt"


def test_chunk_uploader_iter_text_chunks_keeps_lines_together() -> None:
    uploader = api_service.ChunkUploader(max_chunk_size=10)
    file_obj = BytesIO(b"alpha\nbeta\ngamma\n")

    chunks = list(uploader.iter_text_chunks(file_obj))

    assert chunks == [
        b"alpha\n",
        b"beta\n",
        b"gamma\n",
    ]


def test_chunk_uploader_allows_single_oversized_line() -> None:
    uploader = api_service.ChunkUploader(max_chunk_size=5)
    file_obj = BytesIO(b"very-long-line\nshort\n")

    chunks = list(uploader.iter_text_chunks(file_obj))

    assert chunks == [
        b"very-long-line\n",
        b"short\n",
    ]


def test_chunk_uploader_uploads_chunks_and_returns_manifest() -> None:
    uploads = []
    uploader = api_service.ChunkUploader(
        bucket="default-bucket",
        max_chunk_size=12,
        upload_func=lambda data, bucket, key, content_type: uploads.append((data, bucket, key, content_type)),
    )
    file_obj = BytesIO(b"alpha\nbeta\ngamma\n")

    manifest = uploader.upload_chunks("job-1", file_obj, bucket="bucket-1")

    assert uploads == [
        (b"alpha\nbeta\n", "bucket-1", "jobs/job-1/chunks/part_00000.txt", "text/plain"),
        (b"gamma\n", "bucket-1", "jobs/job-1/chunks/part_00001.txt", "text/plain"),
    ]
    assert manifest == {
        "chunk_count": 2,
        "total_bytes": len(b"alpha\nbeta\ngamma\n"),
        "chunks": [
            {
                "part_index": 0,
                "key": "jobs/job-1/chunks/part_00000.txt",
                "size_bytes": len(b"alpha\nbeta\n"),
                "sha256": hashlib.sha256(b"alpha\nbeta\n").hexdigest(),
            },
            {
                "part_index": 1,
                "key": "jobs/job-1/chunks/part_00001.txt",
                "size_bytes": len(b"gamma\n"),
                "sha256": hashlib.sha256(b"gamma\n").hexdigest(),
            },
        ],
    }


def test_chunk_uploader_rejects_empty_file() -> None:
    uploader = api_service.ChunkUploader(upload_func=lambda *args, **kwargs: None)

    with pytest.raises(ValueError, match="Input file is empty"):
        uploader.upload_chunks("job-1", BytesIO(b""))


def test_create_from_chunks_manifest_saves_uploaded_job(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = RecordingRepository()
    service = api_service.JobService(
        repository=repository,
        uploader=api_service.ChunkUploader(upload_func=lambda *args, **kwargs: None),
        event_publisher=RecordingPublisher(),
        bucket="bucket-1",
    )
    monkeypatch.setattr(api_service.time, "time", lambda: 1000.0)

    job = service.create_from_chunks_manifest(
        job_id="job-1",
        original_filename="../../input.txt",
        bucket="bucket-1",
        manifest={
            "chunk_count": 1,
            "total_bytes": 6,
            "chunks": [
                {
                    "part_index": 0,
                    "key": "jobs/job-1/chunks/part_00000.txt",
                    "size_bytes": 6,
                    "sha256": hashlib.sha256(b"hello\n").hexdigest(),
                },
            ],
        },
    )

    assert job["job_id"] == "job-1"
    assert job["status"] == JobStatus.UPLOADED.value
    assert job["original_filename"] == "input.txt"
    assert job["bucket"] == "bucket-1"
    assert job["submitted_at"] == 1000.0
    assert repository.saved == [job]


def test_create_from_upload_uploads_chunks_saves_job_and_publishes_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = RecordingRepository()
    publisher = RecordingPublisher()
    uploads = []
    uploader = api_service.ChunkUploader(
        max_chunk_size=20,
        upload_func=lambda data, bucket, key, content_type: uploads.append((data, bucket, key, content_type)),
    )
    service = api_service.JobService(
        repository=repository,
        uploader=uploader,
        event_publisher=publisher,
        bucket="default-bucket",
    )
    monkeypatch.setattr(api_service.uuid, "uuid4", lambda: "job-1")
    monkeypatch.setattr(api_service.time, "time", lambda: 2000.0)

    job = service.create_from_upload(BytesIO(b"hello\nworld\n"), "input.txt", bucket="bucket-1")

    assert job["job_id"] == "job-1"
    assert job["chunk_count"] == 1
    assert uploads == [
        (b"hello\nworld\n", "bucket-1", "jobs/job-1/chunks/part_00000.txt", "text/plain"),
    ]
    assert len(publisher.events) == 1
    event = publisher.events[0]
    assert event.job_id == "job-1"
    assert event.bucket == "bucket-1"
    assert event.chunks_prefix == "jobs/job-1/chunks/"
    assert event.created_at == 2000.0


def test_get_job_loads_from_repository() -> None:
    repository = RecordingRepository()
    repository.jobs["job-1"] = {"job_id": "job-1"}
    service = api_service.JobService(
        repository=repository,
        uploader=api_service.ChunkUploader(upload_func=lambda *args, **kwargs: None),
        event_publisher=RecordingPublisher(),
    )

    assert service.get_job("job-1") == {"job_id": "job-1"}
    assert service.get_job("missing") is None

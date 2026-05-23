import json

import pytest
from pydantic import ValidationError

from libs.models import (
    ChunkInfo,
    Job,
    JobCancelledEvent,
    JobStatus,
    JobUploadedEvent,
    TaskCompletedEvent,
    TaskOutputFile,
    TaskOutputManifest,
    TaskType,
    WorkerTask,
)
from libs.storage_client.paths import map_output_key


def test_worker_task_serializes_task_type_as_string() -> None:
    task = WorkerTask(
        job_id="job-1",
        task_id="map-1",
        type=TaskType.MAP,
        address="jobs/job-1/chunks/part_00000.txt",
        bucket="bucket-1",
        created_at=123.45,
    )

    assert json.loads(task.model_dump_json()) == {
        "job_id": "job-1",
        "task_id": "map-1",
        "type": "map",
        "address": "jobs/job-1/chunks/part_00000.txt",
        "storage": "minio",
        "bucket": "bucket-1",
        "created_at": 123.45,
        "part_num": None,
    }


def test_task_completed_event_accepts_reduce_part_number() -> None:
    event = TaskCompletedEvent(
        job_id="job-1",
        task_id="reduce-2",
        task_type=TaskType.REDUCE,
        worker_id="worker-1",
        bucket="bucket-1",
        completed_at=123.45,
        part_num=2,
    )

    assert event.task_type == TaskType.REDUCE
    assert event.part_num == 2


def test_task_output_manifest_serializes_task_outputs() -> None:
    manifest = TaskOutputManifest(
        job_id="job-1",
        task_id="map-1",
        task_type=TaskType.MAP,
        bucket="bucket-1",
        created_at=123.45,
        outputs=[
            TaskOutputFile(
                part_num=0,
                key=map_output_key("job-1", "map-1", "part_0_0.jsonl"),
            )
        ],
    )

    assert json.loads(manifest.model_dump_json()) == {
        "job_id": "job-1",
        "task_id": "map-1",
        "task_type": "map",
        "bucket": "bucket-1",
        "created_at": 123.45,
        "outputs": [
            {
                "part_num": 0,
                "key": map_output_key("job-1", "map-1", "part_0_0.jsonl"),
            }
        ],
    }


def test_job_uploaded_event_preserves_chunk_prefix() -> None:
    event = JobUploadedEvent(
        job_id="job-1",
        bucket="bucket-1",
        chunks_prefix="jobs/job-1/chunks/",
        created_at=123.45,
    )

    assert event.chunks_prefix == "jobs/job-1/chunks/"


def test_job_cancelled_event_serializes_reason() -> None:
    event = JobCancelledEvent(
        job_id="job-1",
        reason="task timed out",
        cancelled_at=123.45,
    )

    assert json.loads(event.model_dump_json()) == {
        "job_id": "job-1",
        "reason": "task timed out",
        "cancelled_at": 123.45,
    }


def test_job_model_contains_chunk_metadata_and_defaults() -> None:
    job = Job(
        job_id="job-1",
        status=JobStatus.UPLOADED,
        original_filename="input.txt",
        bucket="bucket-1",
        chunk_count=1,
        total_bytes=6,
        chunks=[
            ChunkInfo(
                part_index=0,
                key="jobs/job-1/chunks/part_00000.txt",
                size_bytes=6,
                sha256="hash",
            )
        ],
        submitted_at=123.45,
    )

    assert job.storage == "minio"
    assert job.completed_at is None
    assert job.result_key is None
    assert job.chunks[0].part_index == 0


@pytest.mark.parametrize(
    "factory",
    [
        lambda: ChunkInfo(part_index=-1, key="key", size_bytes=1, sha256="hash"),
        lambda: ChunkInfo(part_index=0, key="key", size_bytes=-1, sha256="hash"),
        lambda: WorkerTask(
            job_id="job-1",
            task_id="reduce-1",
            type=TaskType.REDUCE,
            address="1",
            bucket="bucket-1",
            created_at=123.45,
            part_num=-1,
        ),
        lambda: TaskCompletedEvent(
            job_id="job-1",
            task_id="reduce-1",
            task_type=TaskType.REDUCE,
            worker_id="worker-1",
            bucket="bucket-1",
            completed_at=123.45,
            part_num=-1,
        ),
        lambda: TaskOutputFile(part_num=-1, key="key"),
        lambda: Job(
            job_id="job-1",
            status=JobStatus.UPLOADED,
            original_filename="input.txt",
            bucket="bucket-1",
            chunk_count=-1,
            total_bytes=0,
            chunks=[],
            submitted_at=123.45,
        ),
    ],
)
def test_models_reject_negative_count_fields(factory) -> None:
    with pytest.raises(ValidationError):
        factory()

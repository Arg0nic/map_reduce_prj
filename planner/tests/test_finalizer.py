import json

import pytest

import planner.finalizer as finalizer
from libs.models import JobStatus, TaskOutputFile, TaskOutputManifest, TaskType
from libs.storage_client.paths import result_key, task_output_key


def make_reduce_manifest(task_id: str, part_num: int, key: str) -> TaskOutputManifest:
    return TaskOutputManifest(
        job_id="job-1",
        task_id=task_id,
        task_type=TaskType.REDUCE,
        bucket="bucket-1",
        created_at=123.45,
        outputs=[TaskOutputFile(part_num=part_num, key=key)],
    )


def test_collect_reduce_results_merges_jsonl_reduce_outputs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    key_1 = task_output_key("job-1", "reduce-1", "reduced_1.jsonl")
    key_0 = task_output_key("job-1", "reduce-0", "reduced_0.jsonl")
    objects = {
        key_1: b'{"beta": "2"}\n{"alpha": 1}\n\n',
        key_0: b'{"alpha": 3}\n{"gamma": 4}\n',
    }
    monkeypatch.setattr(
        finalizer,
        "list_task_output_manifests",
        lambda bucket, job_id, task_type: [
            make_reduce_manifest("reduce-1", 1, key_1),
            make_reduce_manifest("reduce-0", 0, key_0),
        ],
    )
    monkeypatch.setattr(finalizer, "read_object_bytes", lambda bucket, key: objects[key])

    result = finalizer.collect_reduce_results("bucket-1", "job-1")

    assert result == {
        "alpha": 4,
        "beta": 2,
        "gamma": 4,
    }
    assert list(result.keys()) == ["alpha", "beta", "gamma"]


def test_collect_reduce_results_rejects_missing_reduce_outputs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(finalizer, "list_task_output_manifests", lambda bucket, job_id, task_type: [])

    with pytest.raises(FileNotFoundError, match="No reduce output manifests"):
        finalizer.collect_reduce_results("bucket-1", "job-1")


def test_collect_reduce_results_propagates_invalid_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    key = task_output_key("job-1", "reduce-0", "reduced_0.jsonl")
    monkeypatch.setattr(
        finalizer,
        "list_task_output_manifests",
        lambda bucket, job_id, task_type: [make_reduce_manifest("reduce-0", 0, key)],
    )
    monkeypatch.setattr(finalizer, "read_object_bytes", lambda bucket, requested_key: b"not-json\n")

    with pytest.raises(json.JSONDecodeError):
        finalizer.collect_reduce_results("bucket-1", "job-1")


def test_finalize_job_uploads_result_and_updates_job_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    uploads = []
    updates = []

    class FakeRepository:
        def update(self, job_id: str, patch: dict) -> dict:
            updates.append((job_id, patch))
            return {"job_id": job_id, **patch}

    monkeypatch.setattr(finalizer, "collect_reduce_results", lambda bucket, job_id: {"beta": 2, "alpha": 1})
    monkeypatch.setattr(
        finalizer,
        "upload_bytes",
        lambda data, bucket, key, content_type: uploads.append((data, bucket, key, content_type)),
    )
    monkeypatch.setattr(finalizer, "JOB_REPOSITORY", FakeRepository())
    monkeypatch.setattr(finalizer.time, "time", lambda: 500.0)

    key = finalizer.finalize_job("job-1", "bucket-1")

    assert key == result_key("job-1")
    assert uploads == [
        (
            b'{"alpha": 1, "beta": 2}',
            "bucket-1",
            result_key("job-1"),
            "application/json",
        ),
    ]
    assert updates == [
        (
            "job-1",
            {
                "status": JobStatus.DONE.value,
                "completed_at": 500.0,
                "result_key": result_key("job-1"),
                "planner_status": "done",
                "planner_message": "Job completed.",
            },
        ),
    ]


def test_finalize_job_rejects_missing_job_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class MissingRepository:
        def update(self, job_id: str, patch: dict) -> None:
            return None

    monkeypatch.setattr(finalizer, "collect_reduce_results", lambda bucket, job_id: {"alpha": 1})
    monkeypatch.setattr(finalizer, "upload_bytes", lambda *args, **kwargs: None)
    monkeypatch.setattr(finalizer, "JOB_REPOSITORY", MissingRepository())

    with pytest.raises(FileNotFoundError, match="Job metadata not found"):
        finalizer.finalize_job("job-1", "bucket-1")

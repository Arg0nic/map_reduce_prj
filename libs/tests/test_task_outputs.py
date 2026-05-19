import json

from libs.models import TaskOutputFile, TaskOutputManifest, TaskType
from libs.storage_client.paths import task_manifest_key, task_manifests_prefix
from libs.task_outputs import (
    list_task_output_keys_for_part,
    list_task_output_manifests,
    read_task_output_manifest,
    write_task_output_manifest,
)


def make_manifest(task_id: str, task_type: TaskType, part_num: int) -> TaskOutputManifest:
    return TaskOutputManifest(
        job_id="job-1",
        task_id=task_id,
        task_type=task_type,
        bucket="bucket-1",
        created_at=123.45,
        outputs=[
            TaskOutputFile(
                part_num=part_num,
                key=f"jobs/job-1/task_outputs/{task_id}/part_{part_num}_0.jsonl",
            )
        ],
    )


def test_write_task_output_manifest_uploads_commit_file(monkeypatch) -> None:
    uploads = []
    manifest = make_manifest("map-1", TaskType.MAP, 2)

    monkeypatch.setattr(
        "libs.task_outputs.upload_bytes",
        lambda data, bucket, key, content_type: uploads.append((data, bucket, key, content_type)),
    )

    key = write_task_output_manifest("bucket-1", manifest)

    assert key == task_manifest_key("job-1", "map-1")
    data, bucket, uploaded_key, content_type = uploads[0]
    assert bucket == "bucket-1"
    assert uploaded_key == key
    assert content_type == "application/json"
    assert json.loads(data.decode("utf-8"))["task_id"] == "map-1"


def test_read_task_output_manifest_parses_stored_json(monkeypatch) -> None:
    manifest = make_manifest("map-1", TaskType.MAP, 2)
    monkeypatch.setattr(
        "libs.task_outputs.read_object_bytes",
        lambda bucket, key: manifest.model_dump_json().encode("utf-8"),
    )

    result = read_task_output_manifest("bucket-1", task_manifest_key("job-1", "map-1"))

    assert result == manifest


def test_list_task_output_manifests_filters_by_task_type(monkeypatch) -> None:
    map_manifest = make_manifest("map-1", TaskType.MAP, 0)
    reduce_manifest = make_manifest("reduce-0", TaskType.REDUCE, 0)
    objects = {
        task_manifest_key("job-1", "map-1"): map_manifest.model_dump_json().encode("utf-8"),
        task_manifest_key("job-1", "reduce-0"): reduce_manifest.model_dump_json().encode("utf-8"),
    }

    monkeypatch.setattr(
        "libs.task_outputs.list_objects",
        lambda bucket, prefix: [
            task_manifest_key("job-1", "reduce-0"),
            f"{task_manifests_prefix('job-1')}ignored.txt",
            task_manifest_key("job-1", "map-1"),
        ],
    )
    monkeypatch.setattr("libs.task_outputs.read_object_bytes", lambda bucket, key: objects[key])

    result = list_task_output_manifests("bucket-1", "job-1", task_type=TaskType.MAP)

    assert result == [map_manifest]


def test_list_task_output_keys_for_part_returns_committed_keys(monkeypatch) -> None:
    manifests = [
        TaskOutputManifest(
            job_id="job-1",
            task_id="map-1",
            task_type=TaskType.MAP,
            bucket="bucket-1",
            created_at=123.45,
            outputs=[
                TaskOutputFile(part_num=0, key="key-0"),
                TaskOutputFile(part_num=2, key="key-2b"),
            ],
        ),
        TaskOutputManifest(
            job_id="job-1",
            task_id="map-2",
            task_type=TaskType.MAP,
            bucket="bucket-1",
            created_at=123.45,
            outputs=[TaskOutputFile(part_num=2, key="key-2a")],
        ),
    ]
    monkeypatch.setattr("libs.task_outputs.list_task_output_manifests", lambda bucket, job_id, task_type: manifests)

    assert list_task_output_keys_for_part("bucket-1", "job-1", TaskType.MAP, 2) == [
        "key-2a",
        "key-2b",
    ]

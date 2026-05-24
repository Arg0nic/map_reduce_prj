import json

import pytest

import planner.task_planner as task_planner
from libs.models import JobUploadedEvent, TaskOutputFile, TaskOutputManifest, TaskType
from libs.storage_client.paths import map_output_key


class FakeChannel:
    '''
        Minimal RabbitMQ channel fake for task publishing tests.
    '''

    def __init__(self):
        self.published = []

    def basic_publish(self, exchange, routing_key, body, properties):
        self.published.append(
            {
                "exchange": exchange,
                "routing_key": routing_key,
                "body": body,
                "properties": properties,
            }
        )


def test_send_task_publishes_durable_worker_task(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(task_planner.uuid, "uuid4", lambda: "task-uuid")
    monkeypatch.setattr(task_planner.time, "time", lambda: 123.45)
    channel = FakeChannel()

    task = task_planner.send_task(
        channel,
        TaskType.MAP,
        address="jobs/job-1/chunks/part_00000.txt",
        job_id="job-1",
        bucket="bucket-1",
    )

    assert task.task_id == "task-uuid"
    assert task.type == TaskType.MAP
    assert task.created_at == 123.45
    assert len(channel.published) == 1
    published = channel.published[0]
    body = json.loads(published["body"])
    assert published["routing_key"] == task_planner.QUEUE_TASKS
    assert published["properties"].delivery_mode == 2
    assert published["properties"].content_type == "application/json"
    assert body["job_id"] == "job-1"
    assert body["task_id"] == "task-uuid"
    assert body["type"] == "map"
    assert body["address"] == "jobs/job-1/chunks/part_00000.txt"
    assert body["bucket"] == "bucket-1"


def test_list_reduce_part_numbers_discovers_sorted_partition_numbers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        task_planner,
        "list_task_output_manifests",
        lambda bucket, job_id, task_type: [
            TaskOutputManifest(
                job_id="job-1",
                task_id="map-1",
                task_type=TaskType.MAP,
                bucket="bucket-1",
                created_at=123.45,
                outputs=[
                    TaskOutputFile(part_num=2, key=map_output_key("job-1", "map-1", "part_2_0.jsonl")),
                    TaskOutputFile(part_num=0, key=map_output_key("job-1", "map-1", "part_0_0.jsonl")),
                ],
            ),
            TaskOutputManifest(
                job_id="job-1",
                task_id="map-2",
                task_type=TaskType.MAP,
                bucket="bucket-1",
                created_at=123.45,
                outputs=[
                    TaskOutputFile(part_num=2, key=map_output_key("job-1", "map-2", "part_2_0.jsonl")),
                ],
            ),
        ],
    )

    assert task_planner.list_reduce_part_numbers("bucket-1", "job-1") == [0, 2]


def test_create_map_tasks_for_job_creates_one_task_per_sorted_chunk(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    channel = FakeChannel()
    event = JobUploadedEvent(
        job_id="job-1",
        bucket="bucket-1",
        chunks_prefix="jobs/job-1/chunks/",
        created_at=123.45,
    )

    monkeypatch.setattr(
        task_planner,
        "list_objects",
        lambda bucket, prefix: [
            "jobs/job-1/chunks/part_00001.txt",
            "jobs/job-1/chunks/part_00000.txt",
        ],
    )
    monkeypatch.setattr(task_planner.uuid, "uuid4", lambda: "generated-task-id")
    monkeypatch.setattr(task_planner.time, "time", lambda: 200.0)

    tasks = task_planner.create_map_tasks_for_job(channel, event)

    assert [task.address for task in tasks] == [
        "jobs/job-1/chunks/part_00000.txt",
        "jobs/job-1/chunks/part_00001.txt",
    ]
    assert [task.task_id for task in tasks] == [
        "job-1-map-chunk-0",
        "job-1-map-chunk-1",
    ]
    assert [task.type for task in tasks] == [TaskType.MAP, TaskType.MAP]
    assert len(channel.published) == 2


def test_create_map_tasks_for_job_rejects_empty_chunk_prefix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event = JobUploadedEvent(
        job_id="job-1",
        bucket="bucket-1",
        chunks_prefix="jobs/job-1/chunks/",
        created_at=123.45,
    )
    monkeypatch.setattr(task_planner, "list_objects", lambda bucket, prefix: [])

    with pytest.raises(FileNotFoundError, match="No chunks found"):
        task_planner.create_map_tasks_for_job(FakeChannel(), event)


def test_create_reduce_tasks_for_job_creates_deterministic_partition_tasks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    channel = FakeChannel()
    monkeypatch.setattr(task_planner, "list_reduce_part_numbers", lambda bucket, job_id: [0, 3])
    monkeypatch.setattr(task_planner.time, "time", lambda: 300.0)

    tasks = task_planner.create_reduce_tasks_for_job(channel, "job-1", "bucket-1")

    assert [task.task_id for task in tasks] == [
        "job-1-reduce-part-0",
        "job-1-reduce-part-3",
    ]
    assert [task.address for task in tasks] == ["0", "3"]
    assert [task.part_num for task in tasks] == [0, 3]
    assert [task.type for task in tasks] == [TaskType.REDUCE, TaskType.REDUCE]
    assert len(channel.published) == 2


def test_create_reduce_tasks_for_job_rejects_missing_reduce_parts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(task_planner, "list_reduce_part_numbers", lambda bucket, job_id: [])

    with pytest.raises(FileNotFoundError, match="No reduce parts found"):
        task_planner.create_reduce_tasks_for_job(FakeChannel(), "job-1", "bucket-1")

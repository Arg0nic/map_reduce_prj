import time
import uuid

import pika

from libs.models import JobUploadedEvent, TaskType, WorkerTask
from libs.storage_client.client import list_objects
from libs.storage_client.config import settings
from libs.storage_client.paths import map_manifests_prefix
from libs.task_outputs import list_task_output_manifests


QUEUE_TASKS = "tasks"
DEFAULT_BUCKET = settings.DEFAULT_BUCKET or "mapreduce-data"


def send_task(
    ch,
    task_type: TaskType,
    address: str,
    job_id: str,
    task_id: str | None = None,
    storage: str = "minio",
    bucket: str = DEFAULT_BUCKET,
    part_num: int | None = None,
) -> WorkerTask:
    '''
    Publishes a worker task to RabbitMQ.

    WorkerTask is the message contract between planner and workers. The task
    is persisted in RabbitMQ so it can survive broker restarts.
    '''
    task = WorkerTask(
        job_id=job_id,
        task_id=task_id or str(uuid.uuid4()),
        type=task_type,
        address=address,
        storage=storage,
        bucket=bucket,
        created_at=time.time(),
        part_num=part_num,
    )
    body = task.model_dump_json()
    props = pika.BasicProperties(delivery_mode=2, content_type="application/json")
    ch.basic_publish(exchange="", routing_key=QUEUE_TASKS, body=body, properties=props)
    print(f"[Planner] sent task {task.task_id} type={task.type} address={task.address} storage={task.storage}")
    return task


def list_reduce_part_numbers(bucket: str, job_id: str) -> list[int]:
    '''
    Returns reduce partition numbers discovered from committed map outputs.

    Reduce partitions are discovered from map task manifests instead of
    scanning raw shuffle files, so partial uploads without a manifest are
    ignored.
    '''
    part_numbers = set()

    for manifest in list_task_output_manifests(bucket, job_id, task_type=TaskType.MAP):
        for output in manifest.outputs:
            part_numbers.add(output.part_num)

    return sorted(part_numbers)


def create_map_tasks_for_job(ch, event: JobUploadedEvent) -> list[WorkerTask]:
    '''
    Creates one map task for every uploaded chunk object.

    API gateway already uploaded chunks; planner turns every chunk object into
    one independent map task.
    '''
    chunk_keys = sorted(list_objects(event.bucket, event.chunks_prefix))
    if not chunk_keys:
        raise FileNotFoundError(f"No chunks found in {event.bucket}/{event.chunks_prefix}")

    tasks = []
    for chunk_key in chunk_keys:
        task = send_task(
            ch,
            TaskType.MAP,
            address=chunk_key,
            job_id=event.job_id,
            bucket=event.bucket,
        )
        tasks.append(task)

    return tasks


def create_reduce_tasks_for_job(ch, job_id: str, bucket: str) -> list[WorkerTask]:
    '''
    Creates one reduce task for every discovered shuffle partition.

    Each reduce task owns exactly one partition number and reads every shuffle
    file uploaded under that partition prefix.
    '''
    part_numbers = list_reduce_part_numbers(bucket, job_id)
    if not part_numbers:
        raise FileNotFoundError(f"No reduce parts found in {bucket}/{map_manifests_prefix(job_id)}")

    tasks = []
    for part_num in part_numbers:
        task = send_task(
            ch,
            TaskType.REDUCE,
            address=str(part_num),
            job_id=job_id,
            task_id=f"{job_id}-reduce-part-{part_num}",
            bucket=bucket,
            part_num=part_num,
        )
        tasks.append(task)

    return tasks

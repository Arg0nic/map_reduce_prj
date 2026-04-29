import json
import re
import time
import uuid
from dataclasses import dataclass, field

import pika
from pydantic import ValidationError

from libs.models import JobUploadedEvent, TaskCompletedEvent, TaskType, WorkerTask
from libs.storage_client.client import list_objects
from libs.storage_client.config import settings
from libs.storage_client.paths import shuffle_parts_prefix


QUEUE_TASKS = "tasks"
QUEUE_JOBS = "jobs"
HEARTBEAT_QUEUE = "worker.heartbeat"
TASK_COMPLETED_QUEUE = "task.completed"

RABBIT_PASS = "password"
RABBIT_LOGIN = "admin"
RABBIT_HOST = "localhost"
RABBIT_PORT = 5672
DEFAULT_BUCKET = settings.DEFAULT_BUCKET or "mapreduce-data"


@dataclass
class JobPlanState:
    bucket: str
    map_task_ids: set[str]
    completed_map_task_ids: set[str] = field(default_factory=set)
    reduce_task_ids: set[str] = field(default_factory=set)
    completed_reduce_task_ids: set[str] = field(default_factory=set)
    reduce_started: bool = False
    done: bool = False


JOB_STATES: dict[str, JobPlanState] = {}


def send_task(
    ch,
    task_type: TaskType | str,
    address: str,
    job_id: str,
    task_id: str | None = None,
    storage: str = "minio",
    bucket: str = DEFAULT_BUCKET,
    part_num: int | None = None,
) -> WorkerTask:
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
    prefix = shuffle_parts_prefix(job_id)
    keys = list_objects(bucket, prefix)
    part_numbers = set()

    for key in keys:
        match = re.match(rf"{re.escape(prefix)}part_(\d+)/", key)
        if match:
            part_numbers.add(int(match.group(1)))

    return sorted(part_numbers)


def create_map_tasks_for_job(ch, event: JobUploadedEvent) -> list[WorkerTask]:
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
    part_numbers = list_reduce_part_numbers(bucket, job_id)
    if not part_numbers:
        raise FileNotFoundError(f"No reduce parts found in {bucket}/{shuffle_parts_prefix(job_id)}")

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


def start_reduce_phase(ch, job_id: str, state: JobPlanState) -> None:
    tasks = create_reduce_tasks_for_job(ch, job_id, state.bucket)
    state.reduce_task_ids = {task.task_id for task in tasks}
    state.reduce_started = True
    print(f"[Planner] planned {len(tasks)} reduce tasks for job {job_id}")


def handle_map_completed(ch, event: TaskCompletedEvent) -> None:
    state = JOB_STATES.get(event.job_id)
    if state is None:
        print(f"[Planner] completion for unknown job {event.job_id}, ack and skip")
        return

    if event.task_id not in state.map_task_ids:
        print(f"[Planner] unknown map task {event.task_id} for job {event.job_id}, ack and skip")
        return

    if event.task_id in state.completed_map_task_ids:
        print(f"[Planner] duplicate map completion {event.task_id} for job {event.job_id}")
    else:
        state.completed_map_task_ids.add(event.task_id)
        print(
            f"[Planner] map completed for job {event.job_id}: "
            f"{len(state.completed_map_task_ids)}/{len(state.map_task_ids)}"
        )

    if len(state.completed_map_task_ids) == len(state.map_task_ids) and not state.reduce_started:
        print(f"[Planner] all map tasks completed for job {event.job_id}. Starting reduce phase.")
        start_reduce_phase(ch, event.job_id, state)


def handle_reduce_completed(event: TaskCompletedEvent) -> None:
    state = JOB_STATES.get(event.job_id)
    if state is None:
        print(f"[Planner] completion for unknown job {event.job_id}, ack and skip")
        return

    if event.task_id not in state.reduce_task_ids:
        print(f"[Planner] unknown reduce task {event.task_id} for job {event.job_id}, ack and skip")
        return

    if event.task_id in state.completed_reduce_task_ids:
        print(f"[Planner] duplicate reduce completion {event.task_id} for job {event.job_id}")
    else:
        state.completed_reduce_task_ids.add(event.task_id)
        print(
            f"[Planner] reduce completed for job {event.job_id}: "
            f"{len(state.completed_reduce_task_ids)}/{len(state.reduce_task_ids)}"
        )

    if len(state.completed_reduce_task_ids) == len(state.reduce_task_ids) and not state.done:
        state.done = True
        print(f"[Planner] all reduce tasks completed for job {event.job_id}")


def heartbeat_callback(ch, method, properties, body):
    try:
        heartbeat = json.loads(body)
    except Exception:
        print("[Planner] invalid heartbeat message, ack and skip")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    worker_id = heartbeat.get("worker_id", "unknown")
    timestamp = heartbeat.get("ts")

    if timestamp is None:
        print(f"[Planner] heartbeat from {worker_id} without timestamp")
    else:
        readable_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))
        print(f"[Planner] heartbeat from {worker_id} at {readable_time}")

    ch.basic_ack(delivery_tag=method.delivery_tag)


def job_callback(ch, method, properties, body):
    try:
        payload = json.loads(body)
        event = JobUploadedEvent.model_validate(payload)
    except (json.JSONDecodeError, TypeError, ValidationError):
        print("api_gateway sent invalid job event, ack and skip")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    try:
        tasks = create_map_tasks_for_job(ch, event)
    except Exception as exc:
        print(f"[Planner] failed to create tasks for job {event.job_id}: {exc}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        return

    print(f"[Planner] planned {len(tasks)} map tasks for job {event.job_id}")
    JOB_STATES[event.job_id] = JobPlanState(
        bucket=event.bucket,
        map_task_ids={task.task_id for task in tasks},
    )
    ch.basic_ack(delivery_tag=method.delivery_tag)


def task_completed_callback(ch, method, properties, body):
    try:
        payload = json.loads(body)
        event = TaskCompletedEvent.model_validate(payload)
    except (json.JSONDecodeError, TypeError, ValidationError):
        print("[Planner] invalid task completion event, ack and skip")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    try:
        if event.task_type == TaskType.MAP:
            handle_map_completed(ch, event)
        elif event.task_type == TaskType.REDUCE:
            handle_reduce_completed(event)
        else:
            print(f"[Planner] unknown completed task type {event.task_type}, ack and skip")
    except Exception as exc:
        print(f"[Planner] failed to handle task completion for job {event.job_id}: {exc}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        return

    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    credentials = pika.PlainCredentials(RABBIT_LOGIN, RABBIT_PASS)
    params = pika.ConnectionParameters(
        host=RABBIT_HOST,
        port=RABBIT_PORT,
        virtual_host="/",
        credentials=credentials,
    )
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    ch.queue_declare(queue=QUEUE_TASKS, durable=True)
    ch.queue_declare(queue=HEARTBEAT_QUEUE, durable=False)
    ch.queue_declare(queue=QUEUE_JOBS, durable=True)
    ch.queue_declare(queue=TASK_COMPLETED_QUEUE, durable=True)

    ch.basic_consume(queue=QUEUE_JOBS, on_message_callback=job_callback, auto_ack=False)
    ch.basic_consume(queue=HEARTBEAT_QUEUE, on_message_callback=heartbeat_callback, auto_ack=False)
    ch.basic_consume(queue=TASK_COMPLETED_QUEUE, on_message_callback=task_completed_callback, auto_ack=False)
    print("[Planner] listening for jobs, task completions, and worker heartbeats. Press CTRL+C to stop.")

    try:
        ch.start_consuming()
    except KeyboardInterrupt:
        ch.stop_consuming()
    finally:
        conn.close()


if __name__ == "__main__":
    main()

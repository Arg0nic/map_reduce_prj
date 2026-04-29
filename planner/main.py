import json
import time
import uuid

import pika
from pydantic import ValidationError

from libs.models import JobUploadedEvent, TaskType, WorkerTask
from libs.storage_client.client import list_objects


QUEUE_TASKS = "tasks"
QUEUE_JOBS = "jobs"
HEARTBEAT_QUEUE = "worker.heartbeat"

RABBIT_PASS = "password"
RABBIT_LOGIN = "admin"
RABBIT_HOST = "localhost"
RABBIT_PORT = 5672


def send_task(
    ch,
    task_type: TaskType | str,
    address: str,
    job_id: str,
    task_id: str | None = None,
    storage: str = "minio",
    bucket: str = "mapreduce",
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

    ch.basic_consume(queue=QUEUE_JOBS, on_message_callback=job_callback, auto_ack=False)
    ch.basic_consume(queue=HEARTBEAT_QUEUE, on_message_callback=heartbeat_callback, auto_ack=False)
    print("[Planner] listening for jobs and worker heartbeats. Press CTRL+C to stop.")

    try:
        ch.start_consuming()
    except KeyboardInterrupt:
        ch.stop_consuming()
    finally:
        conn.close()


if __name__ == "__main__":
    main()

import json
import logging
import os
import threading
import time
import uuid

import pika

from libs.logging_config import configure_logging, format_log_fields
from libs.models import TaskCompletedEvent, TaskType
from libs.rabbitmq import create_blocking_connection
from libs.storage_client.config import settings as storage_settings
from worker.config import settings as worker_settings
from worker.heartbeat import start_heartbeat_thread
from worker.task_processing import build_task_paths, process_map_task, process_reduce_task


QUEUE_NAME = "tasks"
DEAD_QUEUE_NAME = "tasks.dead"
TASK_COMPLETED_QUEUE = "task.completed"
DEFAULT_BUCKET = storage_settings.DEFAULT_BUCKET or "mapreduce-data"
CURRENT_TASK = None
CURRENT_TASK_LOCK = threading.Lock()
logger = logging.getLogger(__name__)


RABBIT_PASS = worker_settings.RABBIT_PASS
RABBIT_LOGIN = worker_settings.RABBIT_LOGIN
RABBIT_HOST = worker_settings.RABBIT_HOST
RABBIT_PORT = worker_settings.RABBIT_PORT
RABBIT_CONNECT_RETRIES = worker_settings.RABBIT_CONNECT_RETRIES
RABBIT_CONNECT_RETRY_DELAY_SECONDS = worker_settings.RABBIT_CONNECT_RETRY_DELAY_SECONDS

# number of retries for failed tasks
MAX_RETRIES = worker_settings.MAX_RETRIES


def create_worker_id() -> str:
    return os.getenv("HOSTNAME") or str(uuid.uuid4())[:8]


WORKER_ID = create_worker_id()


def set_current_task(task: dict, task_type: TaskType, started_at: float) -> None:
    global CURRENT_TASK
    with CURRENT_TASK_LOCK:
        CURRENT_TASK = {
            "job_id": task.get("job_id"),
            "task_id": task.get("task_id"),
            "type": task_type.value,
            "bucket": task.get("bucket", DEFAULT_BUCKET),
            "started_at": started_at,
            "part_num": task.get("part_num"),
        }


def clear_current_task(task_id: str | None = None) -> None:
    global CURRENT_TASK
    with CURRENT_TASK_LOCK:
        if task_id is None or CURRENT_TASK is None or CURRENT_TASK.get("task_id") == task_id:
            CURRENT_TASK = None


def get_current_task_snapshot() -> dict | None:
    with CURRENT_TASK_LOCK:
        if CURRENT_TASK is None:
            return None
        return dict(CURRENT_TASK)


def publish_task_completed(ch, task: dict, task_type: TaskType) -> None:
    event = TaskCompletedEvent(
        job_id=task["job_id"],
        task_id=task["task_id"],
        task_type=task_type,
        worker_id=WORKER_ID,
        bucket=task.get("bucket", DEFAULT_BUCKET),
        completed_at=time.time(),
        part_num=task.get("part_num"),
    )
    ch.basic_publish(
        exchange="",
        routing_key=TASK_COMPLETED_QUEUE,
        body=event.model_dump_json(),
        properties=pika.BasicProperties(
            delivery_mode=2,
            content_type="application/json",
        ),
    )
    logger.info(
        "notified planner about completed task %s",
        format_log_fields(
            worker_id=WORKER_ID,
            job_id=task["job_id"],
            task_id=task["task_id"],
            task_type=task_type,
            bucket=task.get("bucket", DEFAULT_BUCKET),
            part_num=task.get("part_num"),
        ),
    )


def callback(ch, method, properties, body):
    try:
        task = json.loads(body)
    except Exception:
        logger.warning("invalid task message, ack and skip %s", format_log_fields(worker_id=WORKER_ID))
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    logger.info(
        "picked task %s",
        format_log_fields(
            worker_id=WORKER_ID,
            job_id=task.get("job_id"),
            task_id=task.get("task_id"),
            task_type=task.get("type"),
            part_num=task.get("part_num"),
        ),
    )

    headers = {}
    if properties is not None:
        headers = properties.headers or {}
    attempts = int(headers.get("x-attempts", 0))

    task_paths = build_task_paths(
        task.get("job_id") or "unknown",
        task.get("task_id") or "unknown",
    )

    try:
        task_type = TaskType(task.get("type"))
        set_current_task(task, task_type, started_at=time.time())
        if task_type == TaskType.MAP:
            process_map_task(task, task_paths, worker_id=WORKER_ID)
        elif task_type == TaskType.REDUCE:
            process_reduce_task(task, task_paths, worker_id=WORKER_ID)
        else:
            raise ValueError(f"Unknown task type: {task_type}")
        publish_task_completed(ch, task, task_type)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        logger.info(
            "completed task %s",
            format_log_fields(
                worker_id=WORKER_ID,
                job_id=task.get("job_id"),
                task_id=task.get("task_id"),
                task_type=task_type,
            ),
        )

    except Exception as exc:
        logger.exception(
            "error processing task %s",
            format_log_fields(
                worker_id=WORKER_ID,
                job_id=task.get("job_id"),
                task_id=task.get("task_id"),
                attempts=attempts,
            ),
        )

        next_attempt = attempts + 1
        headers["x-attempts"] = next_attempt

        if next_attempt >= MAX_RETRIES:
            ch.basic_publish(
                exchange="",
                routing_key=DEAD_QUEUE_NAME,
                body=body,
                properties=pika.BasicProperties(headers=headers, delivery_mode=2),
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logger.error(
                "sent task to dead queue %s",
                format_log_fields(
                    worker_id=WORKER_ID,
                    job_id=task.get("job_id"),
                    task_id=task.get("task_id"),
                    attempts=next_attempt,
                ),
            )
        else:
            ch.basic_publish(
                exchange="",
                routing_key=QUEUE_NAME,
                body=body,
                properties=pika.BasicProperties(headers=headers, delivery_mode=2),
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logger.warning(
                "requeued task after failure %s",
                format_log_fields(
                    worker_id=WORKER_ID,
                    job_id=task.get("job_id"),
                    task_id=task.get("task_id"),
                    attempt=next_attempt,
                ),
            )
    finally:
        clear_current_task(task.get("task_id"))


def main():
    configure_logging("worker")

    conn = create_blocking_connection(
        rabbit_login=RABBIT_LOGIN,
        rabbit_pass=RABBIT_PASS,
        rabbit_host=RABBIT_HOST,
        rabbit_port=RABBIT_PORT,
        service_name=f"Worker {WORKER_ID}",
        retries=RABBIT_CONNECT_RETRIES,
        retry_delay_seconds=RABBIT_CONNECT_RETRY_DELAY_SECONDS,
    )
    ch = conn.channel()

    ch.queue_declare(queue=QUEUE_NAME, durable=True)
    ch.queue_declare(queue=DEAD_QUEUE_NAME, durable=True)
    ch.queue_declare(queue=TASK_COMPLETED_QUEUE, durable=True)
    ch.basic_qos(prefetch_count=1)
    ch.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=False)

    logger.info("waiting for tasks %s", format_log_fields(worker_id=WORKER_ID))
    try:
        start_heartbeat_thread(
            worker_id=WORKER_ID,
            rabbit_login=RABBIT_LOGIN,
            rabbit_pass=RABBIT_PASS,
            rabbit_host=RABBIT_HOST,
            rabbit_port=RABBIT_PORT,
            task_snapshot_provider=get_current_task_snapshot,
        )
        ch.start_consuming()
    except KeyboardInterrupt:
        ch.stop_consuming()
    finally:
        conn.close()


if __name__ == "__main__":
    main()

import json
import threading
import time

import pika
from pydantic import ValidationError

from planner.config import settings
from libs.job_repository import create_job_repository
from libs.models import JobUploadedEvent, TaskCompletedEvent, WorkerTask
from libs.rabbitmq import create_blocking_connection
from libs.task_repository import create_task_repository
from planner.service import PlannerService
from planner.task_planner import QUEUE_TASKS


QUEUE_JOBS = "jobs"
HEARTBEAT_QUEUE = "worker.heartbeat"
TASK_COMPLETED_QUEUE = "task.completed"
DEAD_TASK_QUEUE = "tasks.dead"
RUNNING_TASK_TIMEOUT_SECONDS = settings.RUNNING_TASK_TIMEOUT_SECONDS
RUNNING_TASK_TIMEOUT_CHECK_SECONDS = settings.RUNNING_TASK_TIMEOUT_CHECK_SECONDS

RABBIT_PASS = settings.RABBIT_PASS
RABBIT_LOGIN = settings.RABBIT_LOGIN
RABBIT_HOST = settings.RABBIT_HOST
RABBIT_PORT = settings.RABBIT_PORT
RABBIT_CONNECT_RETRIES = settings.RABBIT_CONNECT_RETRIES
RABBIT_CONNECT_RETRY_DELAY_SECONDS = settings.RABBIT_CONNECT_RETRY_DELAY_SECONDS


PLANNER_SERVICE = None


def get_planner_service() -> PlannerService:
    global PLANNER_SERVICE
    if PLANNER_SERVICE is None:
        PLANNER_SERVICE = PlannerService(
            task_repository=create_task_repository(),
            job_repository=create_job_repository(),
        )
    return PLANNER_SERVICE


def heartbeat_callback(ch, method, properties, body):
    '''
    Handles worker heartbeat messages.

    Heartbeats are advisory signals: planner logs them, but does not drive
    task scheduling from them yet.
    '''
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

    if isinstance(heartbeat.get("current_task"), dict):
        try:
            get_planner_service().handle_worker_heartbeat(heartbeat)
        except Exception as exc:
            print(f"[Planner] failed to handle heartbeat from {worker_id}: {exc}")

    ch.basic_ack(delivery_tag=method.delivery_tag)


def job_callback(ch, method, properties, body):
    '''
    Handles uploaded-job events from API Gateway.

    RabbitMQ callbacks stay thin here: parse the event, delegate business
    logic to PlannerService, then ack/nack the broker message.
    '''
    try:
        payload = json.loads(body)
        event = JobUploadedEvent.model_validate(payload)
    except (json.JSONDecodeError, TypeError, ValidationError):
        print("api_gateway sent invalid job event, ack and skip")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    try:
        get_planner_service().handle_job_uploaded(ch, event)
    except Exception as exc:
        print(f"[Planner] failed to create tasks for job {event.job_id}: {exc}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        return

    ch.basic_ack(delivery_tag=method.delivery_tag)


def task_completed_callback(ch, method, properties, body):
    '''
    Handles task completion events from workers.

    Worker completion events are durable pipeline signals. If handling fails,
    the event is requeued so planner can retry the phase transition later.
    '''
    try:
        payload = json.loads(body)
        event = TaskCompletedEvent.model_validate(payload)
    except (json.JSONDecodeError, TypeError, ValidationError):
        print("[Planner] invalid task completion event, ack and skip")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    try:
        get_planner_service().handle_task_completed(ch, event)
    except Exception as exc:
        print(f"[Planner] failed to handle task completion for job {event.job_id}: {exc}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        return

    ch.basic_ack(delivery_tag=method.delivery_tag)


def task_dead_callback(ch, method, properties, body):
    '''
    Handles tasks that exhausted worker retries and reached the dead queue.
    '''
    try:
        payload = json.loads(body)
        task = WorkerTask.model_validate(payload).model_dump(mode="json")
    except (json.JSONDecodeError, TypeError, ValidationError):
        print("[Planner] invalid dead task message, ack and skip")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    try:
        get_planner_service().handle_task_dead(task)
    except Exception as exc:
        print(f"[Planner] failed to handle dead task {task.get('task_id')}: {exc}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        return

    ch.basic_ack(delivery_tag=method.delivery_tag)


def start_task_timeout_monitor():
    '''
    Periodically fails tasks that stayed running past the configured timeout.
    '''
    def monitor() -> None:
        while True:
            time.sleep(RUNNING_TASK_TIMEOUT_CHECK_SECONDS)
            try:
                get_planner_service().fail_timed_out_tasks(RUNNING_TASK_TIMEOUT_SECONDS)
            except Exception as exc:
                print(f"[Planner] failed to check running task timeouts: {exc}")

    thread = threading.Thread(target=monitor, daemon=True)
    thread.start()
    return thread


def main():
    '''
    Starts the planner RabbitMQ consumer loop.

    Planner owns the consumer side of orchestration queues and publishes
    worker tasks through the same RabbitMQ channel.
    '''
    conn = create_blocking_connection(
        rabbit_login=RABBIT_LOGIN,
        rabbit_pass=RABBIT_PASS,
        rabbit_host=RABBIT_HOST,
        rabbit_port=RABBIT_PORT,
        service_name="Planner",
        retries=RABBIT_CONNECT_RETRIES,
        retry_delay_seconds=RABBIT_CONNECT_RETRY_DELAY_SECONDS,
    )
    ch = conn.channel()
    ch.queue_declare(queue=QUEUE_TASKS, durable=True)
    ch.queue_declare(queue=HEARTBEAT_QUEUE, durable=False)
    ch.queue_declare(queue=QUEUE_JOBS, durable=True)
    ch.queue_declare(queue=TASK_COMPLETED_QUEUE, durable=True)
    ch.queue_declare(queue=DEAD_TASK_QUEUE, durable=True)

    ch.basic_consume(queue=QUEUE_JOBS, on_message_callback=job_callback, auto_ack=False)
    ch.basic_consume(queue=HEARTBEAT_QUEUE, on_message_callback=heartbeat_callback, auto_ack=False)
    ch.basic_consume(queue=TASK_COMPLETED_QUEUE, on_message_callback=task_completed_callback, auto_ack=False)
    ch.basic_consume(queue=DEAD_TASK_QUEUE, on_message_callback=task_dead_callback, auto_ack=False)
    start_task_timeout_monitor()
    print("[Planner] listening for jobs, task completions, dead tasks, and worker heartbeats. Press CTRL+C to stop.")

    try:
        ch.start_consuming()
    except KeyboardInterrupt:
        ch.stop_consuming()
    finally:
        conn.close()


if __name__ == "__main__":
    main()

import json
import logging
import threading
import time

from pika.exceptions import ChannelClosedByBroker
from pydantic import ValidationError

from libs.heartbeat_queue import (
    HEARTBEAT_MESSAGE_TTL_MS,
    HEARTBEAT_QUEUE,
    declare_heartbeat_queue,
    purge_heartbeat_queue,
)
from libs.job_repository import create_job_repository
from libs.logging_config import configure_logging, format_log_fields
from libs.models import JobUploadedEvent, TaskCompletedEvent, WorkerHeartbeat, WorkerTask
from libs.rabbitmq import create_blocking_connection
from libs.task_repository import create_task_repository
from libs.worker_repository import create_worker_repository
from planner.config import settings
from planner.service import PlannerService
from planner.task_planner import QUEUE_TASKS


QUEUE_JOBS = "jobs"
TASK_COMPLETED_QUEUE = "task.completed"
DEAD_TASK_QUEUE = "tasks.dead"
RUNNING_TASK_TIMEOUT_SECONDS = settings.RUNNING_TASK_TIMEOUT_SECONDS
RUNNING_TASK_TIMEOUT_CHECK_SECONDS = settings.RUNNING_TASK_TIMEOUT_CHECK_SECONDS
WORKER_HEARTBEAT_TIMEOUT_SECONDS = settings.WORKER_HEARTBEAT_TIMEOUT_SECONDS
WORKER_HEARTBEAT_TIMEOUT_CHECK_SECONDS = settings.WORKER_HEARTBEAT_TIMEOUT_CHECK_SECONDS

RABBIT_PASS = settings.RABBIT_PASS
RABBIT_LOGIN = settings.RABBIT_LOGIN
RABBIT_HOST = settings.RABBIT_HOST
RABBIT_PORT = settings.RABBIT_PORT
RABBIT_CONNECT_RETRIES = settings.RABBIT_CONNECT_RETRIES
RABBIT_CONNECT_RETRY_DELAY_SECONDS = settings.RABBIT_CONNECT_RETRY_DELAY_SECONDS


PLANNER_SERVICE = None
logger = logging.getLogger(__name__)


def get_planner_service() -> PlannerService:
    global PLANNER_SERVICE
    if PLANNER_SERVICE is None:
        PLANNER_SERVICE = PlannerService(
            task_repository=create_task_repository(),
            job_repository=create_job_repository(),
            worker_repository=create_worker_repository(),
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
        event = WorkerHeartbeat.model_validate(heartbeat)
    except (json.JSONDecodeError, TypeError, ValidationError):
        logger.warning("invalid heartbeat message, ack and skip")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    worker_id = event.worker_id
    timestamp = event.ts

    readable_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))
    logger.info(
        "worker heartbeat %s",
        format_log_fields(worker_id=worker_id, timestamp=readable_time),
    )

    try:
        get_planner_service().handle_worker_heartbeat(event)
    except Exception as exc:
        logger.exception("failed to handle heartbeat %s", format_log_fields(worker_id=worker_id))

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
        logger.warning("api_gateway sent invalid job event, ack and skip")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    try:
        get_planner_service().handle_job_uploaded(ch, event)
    except Exception as exc:
        logger.exception("failed to create tasks %s", format_log_fields(job_id=event.job_id))
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
        logger.warning("invalid task completion event, ack and skip")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    try:
        get_planner_service().handle_task_completed(ch, event)
    except Exception as exc:
        logger.exception(
            "failed to handle task completion %s",
            format_log_fields(job_id=event.job_id, task_id=event.task_id),
        )
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
        logger.warning("invalid dead task message, ack and skip")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    try:
        get_planner_service().handle_task_dead(task)
    except Exception as exc:
        logger.exception("failed to handle dead task %s", format_log_fields(task_id=task.get("task_id")))
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
                logger.exception("failed to check running task timeouts")

    thread = threading.Thread(target=monitor, daemon=True)
    thread.start()
    return thread


def start_worker_registry_monitor():
    '''
    Periodically marks workers offline after missing heartbeat timeout.
    '''
    def monitor() -> None:
        while True:
            time.sleep(WORKER_HEARTBEAT_TIMEOUT_CHECK_SECONDS)
            try:
                get_planner_service().mark_stale_workers_offline(WORKER_HEARTBEAT_TIMEOUT_SECONDS)
            except Exception as exc:
                logger.exception("failed to check worker heartbeat timeouts")

    thread = threading.Thread(target=monitor, daemon=True)
    thread.start()
    return thread


def prepare_heartbeat_queue(conn, ch):
    '''
    Declares the transient heartbeat queue and drops stale heartbeat messages.

    Heartbeat messages describe current worker liveness, so old messages are
    not useful after a planner restart. If a local RabbitMQ instance still has
    the old queue declaration without TTL, recreate the queue once.
    '''
    try:
        declare_heartbeat_queue(ch)
    except ChannelClosedByBroker as exc:
        if exc.reply_code != 406:
            raise

        logger.warning(
            "recreating heartbeat queue with TTL %s",
            format_log_fields(queue=HEARTBEAT_QUEUE, ttl_ms=HEARTBEAT_MESSAGE_TTL_MS),
        )
        ch = conn.channel()
        ch.queue_delete(queue=HEARTBEAT_QUEUE)
        declare_heartbeat_queue(ch)

    purge_heartbeat_queue(ch)
    logger.info(
        "prepared heartbeat queue %s",
        format_log_fields(queue=HEARTBEAT_QUEUE, ttl_ms=HEARTBEAT_MESSAGE_TTL_MS),
    )
    return ch


def main():
    '''
    Starts the planner RabbitMQ consumer loop.

    Planner owns the consumer side of orchestration queues and publishes
    worker tasks through the same RabbitMQ channel.
    '''
    configure_logging("planner")

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
    ch = prepare_heartbeat_queue(conn, ch)
    ch.queue_declare(queue=QUEUE_TASKS, durable=True)
    ch.queue_declare(queue=QUEUE_JOBS, durable=True)
    ch.queue_declare(queue=TASK_COMPLETED_QUEUE, durable=True)
    ch.queue_declare(queue=DEAD_TASK_QUEUE, durable=True)

    ch.basic_consume(queue=QUEUE_JOBS, on_message_callback=job_callback, auto_ack=False)
    ch.basic_consume(queue=HEARTBEAT_QUEUE, on_message_callback=heartbeat_callback, auto_ack=False)
    ch.basic_consume(queue=TASK_COMPLETED_QUEUE, on_message_callback=task_completed_callback, auto_ack=False)
    ch.basic_consume(queue=DEAD_TASK_QUEUE, on_message_callback=task_dead_callback, auto_ack=False)
    start_task_timeout_monitor()
    start_worker_registry_monitor()
    logger.info("listening for jobs, task completions, dead tasks, and worker heartbeats")

    try:
        ch.start_consuming()
    except KeyboardInterrupt:
        ch.stop_consuming()
    finally:
        conn.close()


if __name__ == "__main__":
    main()

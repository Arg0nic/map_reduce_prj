import logging
import threading
import time
from collections.abc import Callable

import pika

from libs.heartbeat_queue import HEARTBEAT_QUEUE
from libs.logging_config import format_log_fields
from libs.models import WorkerHeartbeat


HEARTBEAT_INTERVAL_SECONDS = 3
logger = logging.getLogger(__name__)


def heartbeat_loop(
    worker_id: str,
    rabbit_login: str,
    rabbit_pass: str,
    rabbit_host: str,
    rabbit_port: int,
    interval_seconds: int = HEARTBEAT_INTERVAL_SECONDS,
    task_snapshot_provider: Callable[[], dict | None] | None = None,
) -> None:
    # Use a dedicated connection for heartbeats so the worker's main consumer
    # channel is not blocked by periodic health messages.
    credentials = pika.PlainCredentials(rabbit_login, rabbit_pass)
    params = pika.ConnectionParameters(
        host=rabbit_host,
        port=rabbit_port,
        credentials=credentials,
    )

    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    get_task_snapshot = task_snapshot_provider or (lambda: None)

    while True:
        message = WorkerHeartbeat(
            worker_id=worker_id,
            ts=time.time(),
            current_task=get_task_snapshot(),
        )

        ch.basic_publish(
            exchange="",
            routing_key=HEARTBEAT_QUEUE,
            body=message.model_dump_json(exclude_none=True),
        )
        time.sleep(interval_seconds)


def start_heartbeat_thread(
    worker_id: str,
    rabbit_login: str,
    rabbit_pass: str,
    rabbit_host: str,
    rabbit_port: int,
    interval_seconds: int = HEARTBEAT_INTERVAL_SECONDS,
    task_snapshot_provider: Callable[[], dict | None] | None = None,
) -> threading.Thread:
    # Heartbeats run in a daemon thread so the worker can exit cleanly when the
    # main process stops consuming tasks.
    logger.info(
        "starting heartbeat thread %s",
        format_log_fields(worker_id=worker_id, interval_seconds=interval_seconds),
    )
    thread = threading.Thread(
        target=heartbeat_loop,
        kwargs={
            "worker_id": worker_id,
            "rabbit_login": rabbit_login,
            "rabbit_pass": rabbit_pass,
            "rabbit_host": rabbit_host,
            "rabbit_port": rabbit_port,
            "interval_seconds": interval_seconds,
            "task_snapshot_provider": task_snapshot_provider,
        },
        daemon=True,
    )
    thread.start()
    return thread

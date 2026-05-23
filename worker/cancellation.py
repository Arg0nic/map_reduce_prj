import json
import threading

import pika
from pydantic import ValidationError

from libs.models import JobCancelledEvent


JOB_CANCELLED_EXCHANGE = "job.cancelled"

_CANCELLED_JOBS = set()
_CANCELLED_JOBS_LOCK = threading.Lock()


def mark_job_cancelled(job_id: str) -> None:
    with _CANCELLED_JOBS_LOCK:
        _CANCELLED_JOBS.add(job_id)


def is_job_cancelled(job_id: str | None) -> bool:
    if not job_id:
        return False

    with _CANCELLED_JOBS_LOCK:
        return job_id in _CANCELLED_JOBS


def clear_cancelled_jobs() -> None:
    with _CANCELLED_JOBS_LOCK:
        _CANCELLED_JOBS.clear()


def cancellation_loop(
    worker_id: str,
    rabbit_login: str,
    rabbit_pass: str,
    rabbit_host: str,
    rabbit_port: int,
) -> None:
    credentials = pika.PlainCredentials(rabbit_login, rabbit_pass)
    params = pika.ConnectionParameters(
        host=rabbit_host,
        port=rabbit_port,
        virtual_host="/",
        credentials=credentials,
    )

    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    ch.exchange_declare(exchange=JOB_CANCELLED_EXCHANGE, exchange_type="fanout", durable=True)
    queue = ch.queue_declare(queue="", exclusive=True)
    queue_name = queue.method.queue
    ch.queue_bind(exchange=JOB_CANCELLED_EXCHANGE, queue=queue_name)

    def callback(ch, method, properties, body):
        try:
            event = JobCancelledEvent.model_validate(json.loads(body))
        except (json.JSONDecodeError, TypeError, ValidationError):
            print(f"[{worker_id}] invalid job cancellation event, ack and skip")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        mark_job_cancelled(event.job_id)
        print(f"[{worker_id}] received cancellation for job {event.job_id}: {event.reason}")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    ch.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)
    ch.start_consuming()


def start_cancellation_listener_thread(
    worker_id: str,
    rabbit_login: str,
    rabbit_pass: str,
    rabbit_host: str,
    rabbit_port: int,
) -> threading.Thread:
    thread = threading.Thread(
        target=cancellation_loop,
        kwargs={
            "worker_id": worker_id,
            "rabbit_login": rabbit_login,
            "rabbit_pass": rabbit_pass,
            "rabbit_host": rabbit_host,
            "rabbit_port": rabbit_port,
        },
        daemon=True,
    )
    thread.start()
    return thread

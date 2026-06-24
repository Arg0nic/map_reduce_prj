import logging
import time

import pika

from libs.logging_config import format_log_fields


logger = logging.getLogger(__name__)


def create_blocking_connection(
    rabbit_login: str,
    rabbit_pass: str,
    rabbit_host: str,
    rabbit_port: int,
    service_name: str,
    retries: int = 30,
    retry_delay_seconds: float = 2.0,
) -> pika.BlockingConnection:
    credentials = pika.PlainCredentials(rabbit_login, rabbit_pass)
    params = pika.ConnectionParameters(
        host=rabbit_host,
        port=rabbit_port,
        virtual_host="/",
        credentials=credentials,
    )

    attempts = max(1, retries)
    for attempt in range(1, attempts + 1):
        try:
            return pika.BlockingConnection(params)
        except pika.exceptions.AMQPConnectionError:
            if attempt == attempts:
                raise
            logger.warning(
                "RabbitMQ is not ready, retrying %s",
                format_log_fields(
                    service=service_name,
                    attempt=attempt,
                    max_attempts=attempts,
                    retry_delay_seconds=retry_delay_seconds,
                ),
            )
            time.sleep(retry_delay_seconds)

    raise RuntimeError("Failed to connect to RabbitMQ.")

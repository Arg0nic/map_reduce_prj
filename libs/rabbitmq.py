import time

import pika


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
            print(
                f"[{service_name}] RabbitMQ is not ready "
                f"({attempt}/{attempts}), retrying in {retry_delay_seconds:g}s"
            )
            time.sleep(retry_delay_seconds)

    raise RuntimeError("Failed to connect to RabbitMQ.")

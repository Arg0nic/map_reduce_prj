import logging

import pika

from api_gateway.config import settings
from libs.logging_config import format_log_fields
from libs.models import JobUploadedEvent


QUEUE_JOBS = "jobs"

RABBIT_PASS = settings.RABBIT_PASS
RABBIT_LOGIN = settings.RABBIT_LOGIN
RABBIT_HOST = settings.RABBIT_HOST
RABBIT_PORT = settings.RABBIT_PORT
logger = logging.getLogger(__name__)


class RabbitJobEventPublisher:
    """Publishes API Gateway job events to RabbitMQ."""

    def __init__(
        self,
        queue_name: str = QUEUE_JOBS,
        rabbit_login: str = RABBIT_LOGIN,
        rabbit_pass: str = RABBIT_PASS,
        rabbit_host: str = RABBIT_HOST,
        rabbit_port: int = RABBIT_PORT,
    ):
        self.queue_name = queue_name
        self.rabbit_login = rabbit_login
        self.rabbit_pass = rabbit_pass
        self.rabbit_host = rabbit_host
        self.rabbit_port = rabbit_port

    def publish_job_uploaded(self, event: JobUploadedEvent) -> None:
        credentials = pika.PlainCredentials(self.rabbit_login, self.rabbit_pass)
        params = pika.ConnectionParameters(
            host=self.rabbit_host,
            port=self.rabbit_port,
            virtual_host="/",
            credentials=credentials,
        )

        try:
            conn = pika.BlockingConnection(params)
        except pika.exceptions.AMQPError as exc:
            logger.exception(
                "failed to connect to RabbitMQ for job event publish %s",
                format_log_fields(
                    job_id=event.job_id,
                    queue=self.queue_name,
                    rabbit_host=self.rabbit_host,
                    rabbit_port=self.rabbit_port,
                ),
            )
            raise RuntimeError("Failed to connect to RabbitMQ.") from exc

        try:
            ch = conn.channel()
            ch.queue_declare(queue=self.queue_name, durable=True)
            ch.basic_publish(
                exchange="",
                routing_key=self.queue_name,
                body=event.model_dump_json(),
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    content_type="application/json",
                ),
            )
            logger.info(
                "published job uploaded event %s",
                format_log_fields(
                    job_id=event.job_id,
                    queue=self.queue_name,
                    bucket=event.bucket,
                    chunks_prefix=event.chunks_prefix,
                ),
            )
        except pika.exceptions.AMQPError as exc:
            logger.exception(
                "failed to publish job uploaded event %s",
                format_log_fields(job_id=event.job_id, queue=self.queue_name),
            )
            raise RuntimeError("Failed to publish job event to RabbitMQ.") from exc
        finally:
            conn.close()

# Shared RabbitMQ contract for transient worker heartbeat messages.
HEARTBEAT_QUEUE = "worker.heartbeat"
HEARTBEAT_MESSAGE_TTL_MS = 15_000
HEARTBEAT_QUEUE_ARGUMENTS = {
    "x-message-ttl": HEARTBEAT_MESSAGE_TTL_MS,
}


def declare_heartbeat_queue(channel) -> None:
    channel.queue_declare(
        queue=HEARTBEAT_QUEUE,
        durable=False,
        arguments=dict(HEARTBEAT_QUEUE_ARGUMENTS),
    )


def purge_heartbeat_queue(channel) -> None:
    channel.queue_purge(queue=HEARTBEAT_QUEUE)

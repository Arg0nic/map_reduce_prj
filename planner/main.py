import json
import os
import time
import uuid

import pika

from libs.storage_client.client import upload_file


QUEUE_NAME = "tasks"
HEARTBEAT_QUEUE = "worker.heartbeat"

RABBIT_PASS = "password"
RABBIT_LOGIN = "admin"
RABBIT_HOST = "localhost"
RABBIT_PORT = 5672


# ========== Part for DataManager ==========

def split_and_upload_txt(input_file: str, lines_per_file: int = 1_000_000, bucket="mapreduce", prefix="chunks/") -> list:
    """Split a large txt file into smaller parts and upload them to MinIO."""
    with open(input_file, "r", encoding="utf-8") as source:
        file_count = 0
        lines = []
        file_names = []

        for line in source:
            lines.append(line)
            if len(lines) >= lines_per_file:
                part_file = f"{input_file}_part{file_count}.txt"
                with open(part_file, "w", encoding="utf-8") as part_handle:
                    part_handle.writelines(lines)

                key = f"{prefix}{os.path.basename(part_file)}"
                upload_file(part_file, bucket, key)
                file_names.append(key)

                os.remove(part_file)
                file_count += 1
                lines = []

        if lines:
            part_file = f"{input_file}_part{file_count}.txt"
            with open(part_file, "w", encoding="utf-8") as part_handle:
                part_handle.writelines(lines)

            key = f"{prefix}{os.path.basename(part_file)}"
            upload_file(part_file, bucket, key)
            file_names.append(key)

            os.remove(part_file)

        return file_names


# ========== Part for Task Planner ==========

def send_task(
    ch,
    task_type: str,
    address: str,
    main_task_id,
    task_id,
    storage: str = "minio",
    bucket: str = "mapreduce",
):
    task = {
        "main_task_id": main_task_id,
        "task_id": task_id,
        "type": task_type,
        "address": address,
        "storage": storage,
        "bucket": bucket,
        "created_at": time.time(),
    }
    body = json.dumps(task)
    props = pika.BasicProperties(delivery_mode=2, content_type="application/json")
    ch.basic_publish(exchange="", routing_key=QUEUE_NAME, body=body, properties=props)
    print(f"[Planner] sent task {task['task_id']} type={task_type} address={address} storage={storage}")


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
    ch.queue_declare(queue=QUEUE_NAME, durable=True)
    ch.queue_declare(queue=HEARTBEAT_QUEUE, durable=False)

    main_task_id = "1"
    input_file = r"C:\ovr_pr\large_test_words.txt"
    bucket_name = "mapreduce"

    files = split_and_upload_txt(input_file, lines_per_file=1_000_000, bucket=bucket_name, prefix=f"{main_task_id}/")
    print(files)

    for file_key in files:
        task_id = str(uuid.uuid4())
        send_task(
            ch,
            "map",
            address=file_key,
            main_task_id=main_task_id,
            task_id=task_id,
            storage="minio",
            bucket=bucket_name,
        )

    # for part_num in range(4):
    #     send_task(
    #         ch,
    #         "reduce",
    #         address=str(part_num),
    #         main_task_id=main_task_id,
    #         task_id=str(uuid.uuid4()),
    #         storage="minio",
    #         bucket=bucket_name,
    #     )

    ch.basic_consume(queue=HEARTBEAT_QUEUE, on_message_callback=heartbeat_callback, auto_ack=False)
    print("[Planner] listening for worker heartbeats. Press CTRL+C to stop.")

    try:
        ch.start_consuming()
    except KeyboardInterrupt:
        ch.stop_consuming()
    finally:
        conn.close()


if __name__ == "__main__":
    main()

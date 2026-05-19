import os
import re
import shutil
import time
from dataclasses import dataclass

from libs.models import TaskOutputFile, TaskOutputManifest, TaskType
from libs.storage_client.client import download_file, upload_file
from libs.storage_client.config import settings
from libs.storage_client.paths import map_output_key, reduce_output_key
from libs.task_outputs import list_task_output_keys_for_part, write_task_output_manifest
from worker.loaders import jsonDataSink, jsonDataSource, txtDataSource
from worker.worker import (
    MapExecutor,
    ReduceExecutor,
    ShuffleExecutor,
    WordCountMapper,
    WordCountReducer,
    WordCountShuffler,
)


DEFAULT_BUCKET = settings.DEFAULT_BUCKET or "mapreduce-data"


@dataclass(frozen=True)
class TaskPaths:
    # Keep all task-specific local paths together so the caller does not need
    # to reconstruct directory names in several places.
    task_dir: str
    spill_files_dir: str
    shuffle_files_dir: str
    reduce_output_dir: str


def build_task_paths(job_id: str, task_id: str) -> TaskPaths:
    # Every worker task gets its own isolated local directory tree.
    task_dir = os.path.join("storage", job_id, task_id)
    return TaskPaths(
        task_dir=task_dir,
        spill_files_dir=os.path.join(task_dir, "spill_files"),
        shuffle_files_dir=os.path.join(task_dir, "shuffle_files"),
        reduce_output_dir=os.path.join(task_dir, "reduce_output"),
    )


def download_part_files(job_id: str, part_num: int, bucket: str = DEFAULT_BUCKET) -> str:
    # Reduce tasks rebuild their input locally by downloading every shuffle
    # fragment stored for the selected partition.
    local_dir = os.path.join("storage", job_id, "parts", f"part_{part_num}")
    cleanup_directory(local_dir)
    os.makedirs(local_dir, exist_ok=True)

    objects = list_task_output_keys_for_part(bucket, job_id, TaskType.MAP, part_num)
    if not objects:
        raise FileNotFoundError(f"No committed map output files found for job {job_id} part {part_num}")

    for obj_key in objects:
        key_parts = obj_key.rstrip("/").split("/")
        local_filename = os.path.basename(obj_key)
        if len(key_parts) >= 2:
            local_filename = f"{key_parts[-2]}_{local_filename}"
        local_path = os.path.join(local_dir, local_filename)
        download_file(bucket, obj_key, local_path)

    return local_dir


def download_input_file(task: dict, task_paths: TaskPaths) -> str:
    # Map tasks always work with a local file path, even when the original
    # payload points to an object in MinIO/S3.
    local_path = os.path.join(task_paths.task_dir, "input_file.txt")
    os.makedirs(task_paths.task_dir, exist_ok=True)

    storage_type = task.get("storage")
    if storage_type is None:
        raise ValueError("Task missing 'storage' field (expected 'minio' or 'local').")

    if storage_type == "minio":
        s3_key = task.get("address")
        if not s3_key:
            raise ValueError("Task missing 'address' (S3 object key).")
        bucket = task.get("bucket", DEFAULT_BUCKET)
        download_file(bucket=bucket, key=s3_key, local_path=local_path)
        return local_path

    if storage_type == "local":
        address = task.get("address")
        if not address:
            raise ValueError("Task missing 'address' for local storage.")
        if not os.path.exists(address):
            raise FileNotFoundError(f"Local input file not found: {address}")
        return address

    raise ValueError(f"Unknown storage type: {storage_type}")


def run_map_phase(input_file: str, spill_dir: str) -> None:
    # Run the mapper and write intermediate spill files to the local task dir.
    mapper = WordCountMapper()
    map_executor = MapExecutor(
        mapper,
        jsonDataSink(spill_dir, mode="jsonl"),
        txtDataSource(),
        threshold=5_000,
    )
    map_executor.process(filepath=input_file)


def run_shuffle_phase(spill_dir: str, shuffle_dir: str) -> None:
    # Repartition spill files into per-partition shuffle outputs for reduce.
    shuffler = WordCountShuffler(num_parts=4, flush_threshold=2_000)
    shuffle_executor = ShuffleExecutor(
        shuffler,
        source=jsonDataSource(),
        sink=jsonDataSink(shuffle_dir, mode="jsonl"),
    )
    shuffle_executor.process(spill_dir)


def cleanup_directory(dirpath: str) -> None:
    # Cleanup is intentionally tolerant: missing directories are a normal case
    # in retries and partial execution paths.
    if os.path.isdir(dirpath):
        shutil.rmtree(dirpath)


def prepare_task_workspace(task_paths: TaskPaths) -> None:
    cleanup_directory(task_paths.task_dir)
    os.makedirs(task_paths.task_dir, exist_ok=True)


def process_map_task(task: dict, task_paths: TaskPaths, worker_id: str) -> None:
    job_id = task.get("job_id")
    worker_task_id = task.get("task_id")
    if not job_id:
        raise ValueError("Missing job_id in task")
    if not worker_task_id:
        raise ValueError("Missing task_id in task")

    prepare_task_workspace(task_paths)
    input_file = download_input_file(task, task_paths)

    # Map writes spill files first, then shuffle repartitions them into the
    # per-partition files consumed later by reduce workers.
    run_map_phase(input_file, task_paths.spill_files_dir)
    print("Mapping phase completed. Starting shuffling phase...")

    run_shuffle_phase(task_paths.spill_files_dir, task_paths.shuffle_files_dir)
    print("Shuffling phase completed.")

    cleanup_directory(task_paths.spill_files_dir)

    # Upload shuffle outputs only after local processing succeeds, so retries
    # re-run the whole task instead of publishing partial data.
    upload_shuffle_files(
        job_id=job_id,
        worker_task_id=worker_task_id,
        shuffle_dir=task_paths.shuffle_files_dir,
        task_dir=task_paths.task_dir,
        bucket=task.get("bucket", DEFAULT_BUCKET),
        worker_id=worker_id,
    )


def process_reduce_task(task: dict, task_paths: TaskPaths, worker_id: str) -> None:
    job_id = task.get("job_id")
    worker_task_id = task.get("task_id")
    if not job_id:
        raise ValueError("Missing job_id in task")
    if not worker_task_id:
        raise ValueError("Missing task_id in task")

    address = task.get("address")
    if address is None:
        raise ValueError("Missing address in reduce task")
    part_num = int(address)
    bucket = task.get("bucket", DEFAULT_BUCKET)

    prepare_task_workspace(task_paths)
    print("Starting reducing phase...")
    # The reduce worker aggregates all shuffle fragments that belong to the
    # same partition number, regardless of which map worker produced them.
    part_dir = download_part_files(job_id, part_num, bucket=bucket)
    print(f"Downloaded part files to {part_dir}")

    os.makedirs(task_paths.reduce_output_dir, exist_ok=True)

    reducer = WordCountReducer()
    reduce_executor = ReduceExecutor(
        reducer,
        sink=jsonDataSink(task_paths.reduce_output_dir, mode="jsonl"),
        source=jsonDataSource(),
    )
    reduce_executor.process(part_dir=part_dir, part_num=part_num)

    print("Reducing phase completed.")
    print(f"Reduce output stored in: {task_paths.reduce_output_dir}")

    local_reduce_file = os.path.join(task_paths.reduce_output_dir, f"reduced_{part_num}.jsonl")
    s3_key = reduce_output_key(job_id, worker_task_id, os.path.basename(local_reduce_file))
    upload_file(local_reduce_file, bucket=bucket, key=s3_key)
    print(f"[{worker_id}] uploaded reduce output -> {s3_key}")
    write_task_output_manifest(
        bucket,
        TaskOutputManifest(
            job_id=job_id,
            task_id=worker_task_id,
            task_type=TaskType.REDUCE,
            bucket=bucket,
            created_at=time.time(),
            outputs=[TaskOutputFile(part_num=part_num, key=s3_key)],
        ),
    )


def upload_shuffle_files(
    job_id: str,
    worker_task_id: str,
    shuffle_dir: str,
    task_dir: str,
    bucket: str,
    worker_id: str,
) -> None:
    if not os.path.isdir(shuffle_dir):
        print(f"[{worker_id}] no shuffle dir at {shuffle_dir}, nothing to upload")
        return

    print(f"[{worker_id}] uploading shuffle files from {shuffle_dir} to bucket '{bucket}'")

    upload_failures = []
    outputs = []
    uploaded = 0

    for filename in sorted(os.listdir(shuffle_dir)):
        local_path = os.path.join(shuffle_dir, filename)
        if not os.path.isfile(local_path):
            continue

        part_idx = detect_part_index(filename)
        s3_key = map_output_key(job_id, worker_task_id, filename)

        try:
            upload_file(local_path, bucket=bucket, key=s3_key)
            uploaded += 1
            outputs.append(TaskOutputFile(part_num=part_idx, key=s3_key))
            print(f"[{worker_id}] uploaded {filename} -> {s3_key}")
        except Exception as exc:
            upload_failures.append((filename, str(exc)))
            print(f"[{worker_id}] ERROR uploading {filename}: {exc}")

    if upload_failures:
        print(f"[{worker_id}] upload finished with {len(upload_failures)} failures: {upload_failures}")
        raise RuntimeError(f"Upload errors: {len(upload_failures)} files failed")

    print(f"[{worker_id}] Upload completed. {uploaded} files uploaded.")
    write_task_output_manifest(
        bucket,
        TaskOutputManifest(
            job_id=job_id,
            task_id=worker_task_id,
            task_type=TaskType.MAP,
            bucket=bucket,
            created_at=time.time(),
            outputs=outputs,
        ),
    )

    # Remove local task artifacts only after every upload has been confirmed.
    if os.path.isdir(task_dir):
        try:
            cleanup_directory(task_dir)
            print(f"[{worker_id}] cleaned up local files: {task_dir}")
        except Exception as exc:
            print(f"[{worker_id}] warning: failed to remove {task_dir}: {exc}")
    else:
        print(f"[{worker_id}] nothing to cleanup at {task_dir}")


def detect_part_index(filename: str) -> int:
    # Support both explicit "part_3" style names and simple numeric prefixes.
    match = re.search(r"part[_\-]?(\d+)", filename, flags=re.IGNORECASE)
    if match:
        return int(match.group(1))

    fallback = re.match(r"^(\d+)[_\.\-]", filename)
    if fallback:
        return int(fallback.group(1))

    raise ValueError(f"Could not detect partition index from filename: {filename}")

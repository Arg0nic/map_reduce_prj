import json
import logging
import time

from libs.job_repository import create_job_repository
from libs.logging_config import format_log_fields
from libs.models import JobStatus, TaskType
from libs.storage_client.client import read_object_bytes, upload_bytes
from libs.storage_client.paths import reduce_manifests_prefix, result_key
from libs.task_outputs import list_task_output_manifests


JOB_REPOSITORY = None
logger = logging.getLogger(__name__)


def get_job_repository():
    global JOB_REPOSITORY
    if JOB_REPOSITORY is None:
        JOB_REPOSITORY = create_job_repository()
    return JOB_REPOSITORY


def collect_reduce_results(bucket: str, job_id: str) -> dict[str, int]:
    '''
    Collects committed reduce JSONL output files into one sorted word-count dictionary.

    Reduce workers publish output manifests after successful upload. Planner
    reads only files referenced by those manifests and ignores raw partial
    uploads without a manifest.
    '''
    manifests = list_task_output_manifests(bucket, job_id, task_type=TaskType.REDUCE)
    keys = sorted(output.key for manifest in manifests for output in manifest.outputs)
    if not keys:
        raise FileNotFoundError(f"No reduce output manifests found in {bucket}/{reduce_manifests_prefix(job_id)}")

    logger.info(
        "collecting reduce outputs %s",
        format_log_fields(job_id=job_id, bucket=bucket, output_file_count=len(keys)),
    )

    result = {}
    for key in keys:
        content = read_object_bytes(bucket, key).decode("utf-8")
        for line in content.splitlines():
            if not line.strip():
                continue

            record = json.loads(line)
            for word, count in record.items():
                result[word] = result.get(word, 0) + int(count)

    logger.info(
        "collected reduce outputs %s",
        format_log_fields(job_id=job_id, bucket=bucket, unique_words=len(result)),
    )
    return dict(sorted(result.items(), key=lambda item: item[0]))


def finalize_job(job_id: str, bucket: str) -> str:
    '''
    Builds the final result object and marks the job as done.

    Finalization creates the client-facing result object and updates job
    metadata so the API can return it from /jobs/{job_id}/result.
    '''
    result = collect_reduce_results(bucket, job_id)
    final_result_key = result_key(job_id)
    result_bytes = json.dumps(result, ensure_ascii=False, sort_keys=True).encode("utf-8")

    upload_bytes(
        result_bytes,
        bucket=bucket,
        key=final_result_key,
        content_type="application/json",
    )
    logger.info(
        "uploaded final result %s",
        format_log_fields(
            job_id=job_id,
            bucket=bucket,
            result_key=final_result_key,
            result_bytes=len(result_bytes),
            unique_words=len(result),
        ),
    )

    updated_job = get_job_repository().update(
        job_id,
        {
            "status": JobStatus.DONE.value,
            "completed_at": time.time(),
            "result_key": final_result_key,
            "planner_status": "done",
            "planner_message": "Job completed.",
        },
    )
    if updated_job is None:
        raise FileNotFoundError(f"Job metadata not found for job {job_id}")

    logger.info("marked job done %s", format_log_fields(job_id=job_id, result_key=final_result_key))
    return final_result_key

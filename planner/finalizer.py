import json
import time

from libs.job_repository import LocalJsonJobRepository
from libs.models import JobStatus
from libs.storage_client.client import list_objects, read_object_bytes, upload_bytes
from libs.storage_client.paths import reduce_output_prefix, result_key


JOB_REPOSITORY = LocalJsonJobRepository()


def collect_reduce_results(bucket: str, job_id: str) -> dict[str, int]:
    '''
    Collects reduce JSONL output files into one sorted word-count dictionary.

    Reduce workers write JSONL files. Planner reads every line as a small
    dictionary and folds them into one sorted word-count result.
    '''
    prefix = reduce_output_prefix(job_id)
    keys = sorted(key for key in list_objects(bucket, prefix) if key.endswith(".jsonl"))
    if not keys:
        raise FileNotFoundError(f"No reduce output files found in {bucket}/{prefix}")

    result = {}
    for key in keys:
        content = read_object_bytes(bucket, key).decode("utf-8")
        for line in content.splitlines():
            if not line.strip():
                continue

            record = json.loads(line)
            for word, count in record.items():
                result[word] = result.get(word, 0) + int(count)

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

    updated_job = JOB_REPOSITORY.update(
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

    return final_result_key

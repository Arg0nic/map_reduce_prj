def job_prefix(job_id: str) -> str:
    return f"jobs/{job_id}/"


def chunks_prefix(job_id: str) -> str:
    return f"{job_prefix(job_id)}chunks/"


def shuffle_parts_prefix(job_id: str) -> str:
    return f"{job_prefix(job_id)}parts/"


def shuffle_part_prefix(job_id: str, part_num: int) -> str:
    return f"{shuffle_parts_prefix(job_id)}part_{part_num}/"


def shuffle_part_key(job_id: str, part_num: int, worker_task_id: str, filename: str) -> str:
    return f"{shuffle_part_prefix(job_id, part_num)}{worker_task_id}_{filename}"


def map_outputs_prefix(job_id: str) -> str:
    return f"{job_prefix(job_id)}map_outputs/"


def map_output_prefix(job_id: str, task_id: str) -> str:
    return f"{map_outputs_prefix(job_id)}{task_id}/"


def map_output_key(job_id: str, task_id: str, filename: str) -> str:
    return f"{map_output_prefix(job_id, task_id)}{filename}"


def map_manifests_prefix(job_id: str) -> str:
    return f"{job_prefix(job_id)}map_manifests/"


def map_manifest_key(job_id: str, task_id: str) -> str:
    return f"{map_manifests_prefix(job_id)}{task_id}.json"


def reduce_outputs_prefix(job_id: str) -> str:
    return f"{job_prefix(job_id)}reduce_outputs/"


def reduce_output_prefix(job_id: str, task_id: str) -> str:
    return f"{reduce_outputs_prefix(job_id)}{task_id}/"


def reduce_output_key(job_id: str, task_id: str, filename: str) -> str:
    return f"{reduce_output_prefix(job_id, task_id)}{filename}"


def reduce_manifests_prefix(job_id: str) -> str:
    return f"{job_prefix(job_id)}reduce_manifests/"


def reduce_manifest_key(job_id: str, task_id: str) -> str:
    return f"{reduce_manifests_prefix(job_id)}{task_id}.json"


def result_prefix(job_id: str) -> str:
    return f"{job_prefix(job_id)}result/"


def result_key(job_id: str) -> str:
    return f"{result_prefix(job_id)}result.json"

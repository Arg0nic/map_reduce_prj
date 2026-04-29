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


def reduce_output_prefix(job_id: str) -> str:
    return f"{job_prefix(job_id)}reduce_output/"


def reduce_output_key(job_id: str, part_num: int) -> str:
    return f"{reduce_output_prefix(job_id)}reduced_part_{part_num}.jsonl"

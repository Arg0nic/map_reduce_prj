from libs.models import TaskOutputManifest, TaskType
from libs.storage_client.client import list_objects, read_object_bytes, upload_bytes
from libs.storage_client.paths import (
    map_manifest_key,
    map_manifests_prefix,
    reduce_manifest_key,
    reduce_manifests_prefix,
)


def _manifest_key(job_id: str, task_id: str, task_type: TaskType) -> str:
    if task_type == TaskType.MAP:
        return map_manifest_key(job_id, task_id)
    if task_type == TaskType.REDUCE:
        return reduce_manifest_key(job_id, task_id)
    raise ValueError(f"Unsupported task type: {task_type}")


def _manifest_prefixes(job_id: str, task_type: TaskType | None = None) -> list[str]:
    if task_type == TaskType.MAP:
        return [map_manifests_prefix(job_id)]
    if task_type == TaskType.REDUCE:
        return [reduce_manifests_prefix(job_id)]
    return [map_manifests_prefix(job_id), reduce_manifests_prefix(job_id)]


def write_task_output_manifest(bucket: str, manifest: TaskOutputManifest) -> str:
    key = _manifest_key(manifest.job_id, manifest.task_id, manifest.task_type)
    upload_bytes(
        manifest.model_dump_json().encode("utf-8"),
        bucket=bucket,
        key=key,
        content_type="application/json",
    )
    return key


def read_task_output_manifest(bucket: str, key: str) -> TaskOutputManifest:
    data = read_object_bytes(bucket, key).decode("utf-8")
    return TaskOutputManifest.model_validate_json(data)


def list_task_output_manifests(
    bucket: str,
    job_id: str,
    task_type: TaskType | None = None,
) -> list[TaskOutputManifest]:
    manifests = []

    for prefix in _manifest_prefixes(job_id, task_type=task_type):
        for key in sorted(list_objects(bucket, prefix)):
            if not key.endswith(".json"):
                continue

            manifest = read_task_output_manifest(bucket, key)
            if task_type is None or manifest.task_type == task_type:
                manifests.append(manifest)

    return manifests


def list_task_output_keys_for_part(
    bucket: str,
    job_id: str,
    task_type: TaskType,
    part_num: int,
) -> list[str]:
    keys = []
    for manifest in list_task_output_manifests(bucket, job_id, task_type=task_type):
        for output in manifest.outputs:
            if output.part_num == part_num:
                keys.append(output.key)

    return sorted(keys)

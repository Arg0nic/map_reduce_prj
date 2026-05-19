from libs.storage_client import paths


def test_job_storage_prefixes_are_derived_from_job_id() -> None:
    assert paths.job_prefix("job-1") == "jobs/job-1/"
    assert paths.chunks_prefix("job-1") == "jobs/job-1/chunks/"
    assert paths.shuffle_parts_prefix("job-1") == "jobs/job-1/parts/"
    assert paths.shuffle_part_prefix("job-1", 2) == "jobs/job-1/parts/part_2/"
    assert paths.task_outputs_prefix("job-1") == "jobs/job-1/task_outputs/"
    assert paths.task_output_prefix("job-1", "map-1") == "jobs/job-1/task_outputs/map-1/"
    assert paths.task_manifests_prefix("job-1") == "jobs/job-1/task_manifests/"
    assert paths.reduce_output_prefix("job-1") == "jobs/job-1/reduce_output/"
    assert paths.result_prefix("job-1") == "jobs/job-1/result/"


def test_object_keys_include_phase_specific_file_names() -> None:
    assert paths.shuffle_part_key("job-1", 2, "map-1", "part_2_0.jsonl") == (
        "jobs/job-1/parts/part_2/map-1_part_2_0.jsonl"
    )
    assert paths.task_output_key("job-1", "map-1", "part_2_0.jsonl") == (
        "jobs/job-1/task_outputs/map-1/part_2_0.jsonl"
    )
    assert paths.task_manifest_key("job-1", "map-1") == "jobs/job-1/task_manifests/map-1.json"
    assert paths.reduce_output_key("job-1", 2) == "jobs/job-1/reduce_output/reduced_part_2.jsonl"
    assert paths.result_key("job-1") == "jobs/job-1/result/result.json"

from libs.storage_client import paths


def test_job_storage_prefixes_are_derived_from_job_id() -> None:
    assert paths.job_prefix("job-1") == "jobs/job-1/"
    assert paths.chunks_prefix("job-1") == "jobs/job-1/chunks/"
    assert paths.shuffle_parts_prefix("job-1") == "jobs/job-1/parts/"
    assert paths.shuffle_part_prefix("job-1", 2) == "jobs/job-1/parts/part_2/"
    assert paths.map_outputs_prefix("job-1") == "jobs/job-1/map_outputs/"
    assert paths.map_output_prefix("job-1", "map-1") == "jobs/job-1/map_outputs/map-1/"
    assert paths.map_manifests_prefix("job-1") == "jobs/job-1/map_manifests/"
    assert paths.reduce_outputs_prefix("job-1") == "jobs/job-1/reduce_outputs/"
    assert paths.reduce_output_prefix("job-1", "reduce-1") == "jobs/job-1/reduce_outputs/reduce-1/"
    assert paths.reduce_manifests_prefix("job-1") == "jobs/job-1/reduce_manifests/"
    assert paths.result_prefix("job-1") == "jobs/job-1/result/"


def test_object_keys_include_phase_specific_file_names() -> None:
    assert paths.shuffle_part_key("job-1", 2, "map-1", "part_2_0.jsonl") == (
        "jobs/job-1/parts/part_2/map-1_part_2_0.jsonl"
    )
    assert paths.map_output_key("job-1", "map-1", "part_2_0.jsonl") == (
        "jobs/job-1/map_outputs/map-1/part_2_0.jsonl"
    )
    assert paths.map_manifest_key("job-1", "map-1") == "jobs/job-1/map_manifests/map-1.json"
    assert paths.reduce_output_key("job-1", "reduce-2", "reduced_2.jsonl") == (
        "jobs/job-1/reduce_outputs/reduce-2/reduced_2.jsonl"
    )
    assert paths.reduce_manifest_key("job-1", "reduce-2") == "jobs/job-1/reduce_manifests/reduce-2.json"
    assert paths.result_key("job-1") == "jobs/job-1/result/result.json"

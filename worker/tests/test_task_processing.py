import os
from pathlib import Path

import pytest

import worker.task_processing as task_processing
from libs.storage_client.paths import reduce_output_key, shuffle_part_key, shuffle_part_prefix
from worker.task_processing import TaskPaths


def make_task_paths(tmp_path: Path) -> TaskPaths:
    task_dir = tmp_path / "task"
    return TaskPaths(
        task_dir=str(task_dir),
        spill_files_dir=str(task_dir / "spill_files"),
        shuffle_files_dir=str(task_dir / "shuffle_files"),
        reduce_output_dir=str(task_dir / "reduce_output"),
    )


def test_build_task_paths_returns_isolated_task_directories() -> None:
    paths = task_processing.build_task_paths("job-1", "task-1")

    assert paths == TaskPaths(
        task_dir=os.path.join("storage", "job-1", "task-1"),
        spill_files_dir=os.path.join("storage", "job-1", "task-1", "spill_files"),
        shuffle_files_dir=os.path.join("storage", "job-1", "task-1", "shuffle_files"),
        reduce_output_dir=os.path.join("storage", "job-1", "task-1", "reduce_output"),
    )


@pytest.mark.parametrize(
    ("filename", "expected"),
    [
        ("part_0_0.jsonl", 0),
        ("part-3-0.jsonl", 3),
        ("worker_task_part_12_0.jsonl", 12),
        ("7_extra.jsonl", 7),
        ("unknown.jsonl", "unknown"),
    ],
)
def test_detect_part_index(filename: str, expected: int | str) -> None:
    assert task_processing.detect_part_index(filename) == expected


def test_download_input_file_returns_existing_local_file(tmp_path: Path) -> None:
    input_file = tmp_path / "input.txt"
    input_file.write_text("hello", encoding="utf-8")
    paths = make_task_paths(tmp_path)

    result = task_processing.download_input_file(
        {"storage": "local", "address": str(input_file)},
        paths,
    )

    assert result == str(input_file)
    assert Path(paths.task_dir).is_dir()


def test_download_input_file_downloads_minio_object(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    calls = []
    paths = make_task_paths(tmp_path)

    def fake_download_file(bucket: str, key: str, local_path: str) -> None:
        calls.append((bucket, key, local_path))

    monkeypatch.setattr(task_processing, "download_file", fake_download_file)

    result = task_processing.download_input_file(
        {
            "storage": "minio",
            "address": "jobs/job-1/chunks/part_00000.txt",
            "bucket": "bucket-1",
        },
        paths,
    )

    expected_local_path = os.path.join(paths.task_dir, "input_file.txt")
    assert result == expected_local_path
    assert calls == [
        ("bucket-1", "jobs/job-1/chunks/part_00000.txt", expected_local_path),
    ]


@pytest.mark.parametrize(
    ("task", "error"),
    [
        ({}, "storage"),
        ({"storage": "local"}, "address"),
        ({"storage": "minio"}, "address"),
        ({"storage": "unknown", "address": "input.txt"}, "Unknown storage type"),
    ],
)
def test_download_input_file_rejects_invalid_task(task: dict, error: str, tmp_path: Path) -> None:
    with pytest.raises(ValueError, match=error):
        task_processing.download_input_file(task, make_task_paths(tmp_path))


def test_download_input_file_rejects_missing_local_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        task_processing.download_input_file(
            {"storage": "local", "address": str(tmp_path / "missing.txt")},
            make_task_paths(tmp_path),
        )


def test_download_part_files_downloads_all_objects(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    calls = []

    def fake_list_objects(bucket: str, prefix: str) -> list[str]:
        assert bucket == "bucket-1"
        assert prefix == shuffle_part_prefix("job-1", 2)
        return [
            "jobs/job-1/parts/part_2/map-1_part_2_0.jsonl",
            "jobs/job-1/parts/part_2/map-2_part_2_0.jsonl",
        ]

    def fake_download_file(bucket: str, key: str, local_path: str) -> None:
        calls.append((bucket, key, local_path))

    monkeypatch.setattr(task_processing, "list_objects", fake_list_objects)
    monkeypatch.setattr(task_processing, "download_file", fake_download_file)

    result = task_processing.download_part_files("job-1", 2, bucket="bucket-1")

    assert result == os.path.join("storage", "job-1", "parts", "part_2")
    assert Path(result).is_dir()
    assert calls == [
        (
            "bucket-1",
            "jobs/job-1/parts/part_2/map-1_part_2_0.jsonl",
            os.path.join(result, "map-1_part_2_0.jsonl"),
        ),
        (
            "bucket-1",
            "jobs/job-1/parts/part_2/map-2_part_2_0.jsonl",
            os.path.join(result, "map-2_part_2_0.jsonl"),
        ),
    ]


def test_download_part_files_rejects_empty_storage_prefix(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(task_processing, "list_objects", lambda bucket, prefix: [])

    with pytest.raises(FileNotFoundError, match=shuffle_part_prefix("job-1", 0)):
        task_processing.download_part_files("job-1", 0, bucket="bucket-1")


def test_upload_shuffle_files_uploads_files_and_cleans_task_dir(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    shuffle_dir = tmp_path / "shuffle"
    task_dir = tmp_path / "task"
    shuffle_dir.mkdir()
    task_dir.mkdir()
    (shuffle_dir / "part_1_0.jsonl").write_text("{}", encoding="utf-8")
    (shuffle_dir / "part-2-0.jsonl").write_text("{}", encoding="utf-8")
    (shuffle_dir / "nested").mkdir()
    uploads = []
    cleanups = []

    def fake_upload_file(local_path: str, bucket: str, key: str) -> None:
        uploads.append((os.path.basename(local_path), bucket, key))

    def fake_cleanup_directory(dirpath: str) -> None:
        cleanups.append(dirpath)

    monkeypatch.setattr(task_processing, "upload_file", fake_upload_file)
    monkeypatch.setattr(task_processing, "cleanup_directory", fake_cleanup_directory)

    task_processing.upload_shuffle_files(
        job_id="job-1",
        worker_task_id="map-1",
        shuffle_dir=str(shuffle_dir),
        task_dir=str(task_dir),
        bucket="bucket-1",
        worker_id="worker-1",
    )

    assert uploads == [
        (
            "part-2-0.jsonl",
            "bucket-1",
            shuffle_part_key("job-1", 2, "map-1", "part-2-0.jsonl"),
        ),
        (
            "part_1_0.jsonl",
            "bucket-1",
            shuffle_part_key("job-1", 1, "map-1", "part_1_0.jsonl"),
        ),
    ]
    assert cleanups == [str(task_dir)]


def test_upload_shuffle_files_does_nothing_without_shuffle_dir(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    uploads = []
    cleanups = []
    monkeypatch.setattr(task_processing, "upload_file", lambda *args, **kwargs: uploads.append(args))
    monkeypatch.setattr(task_processing, "cleanup_directory", lambda dirpath: cleanups.append(dirpath))

    task_processing.upload_shuffle_files(
        job_id="job-1",
        worker_task_id="map-1",
        shuffle_dir=str(tmp_path / "missing"),
        task_dir=str(tmp_path / "task"),
        bucket="bucket-1",
        worker_id="worker-1",
    )

    assert uploads == []
    assert cleanups == []


def test_upload_shuffle_files_raises_and_keeps_task_dir_after_upload_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    shuffle_dir = tmp_path / "shuffle"
    task_dir = tmp_path / "task"
    shuffle_dir.mkdir()
    task_dir.mkdir()
    (shuffle_dir / "part_0_0.jsonl").write_text("{}", encoding="utf-8")
    cleanups = []

    def fake_upload_file(local_path: str, bucket: str, key: str) -> None:
        raise RuntimeError("storage unavailable")

    monkeypatch.setattr(task_processing, "upload_file", fake_upload_file)
    monkeypatch.setattr(task_processing, "cleanup_directory", lambda dirpath: cleanups.append(dirpath))

    with pytest.raises(RuntimeError, match="Upload errors"):
        task_processing.upload_shuffle_files(
            job_id="job-1",
            worker_task_id="map-1",
            shuffle_dir=str(shuffle_dir),
            task_dir=str(task_dir),
            bucket="bucket-1",
            worker_id="worker-1",
        )

    assert cleanups == []


def test_process_map_task_runs_map_shuffle_cleanup_and_upload(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls = []
    paths = make_task_paths(tmp_path)
    task = {
        "job_id": "job-1",
        "task_id": "map-1",
        "bucket": "bucket-1",
        "storage": "local",
        "address": "input.txt",
    }

    monkeypatch.setattr(
        task_processing,
        "download_input_file",
        lambda received_task, received_paths: calls.append(("download", received_task, received_paths)) or "input.txt",
    )
    monkeypatch.setattr(
        task_processing,
        "run_map_phase",
        lambda input_file, spill_dir: calls.append(("map", input_file, spill_dir)),
    )
    monkeypatch.setattr(
        task_processing,
        "run_shuffle_phase",
        lambda spill_dir, shuffle_dir: calls.append(("shuffle", spill_dir, shuffle_dir)),
    )
    monkeypatch.setattr(
        task_processing,
        "cleanup_directory",
        lambda dirpath: calls.append(("cleanup", dirpath)),
    )
    monkeypatch.setattr(
        task_processing,
        "upload_shuffle_files",
        lambda **kwargs: calls.append(("upload", kwargs)),
    )

    task_processing.process_map_task(task, paths, worker_id="worker-1")

    assert calls == [
        ("download", task, paths),
        ("map", "input.txt", paths.spill_files_dir),
        ("shuffle", paths.spill_files_dir, paths.shuffle_files_dir),
        ("cleanup", paths.spill_files_dir),
        (
            "upload",
            {
                "job_id": "job-1",
                "worker_task_id": "map-1",
                "shuffle_dir": paths.shuffle_files_dir,
                "task_dir": paths.task_dir,
                "bucket": "bucket-1",
                "worker_id": "worker-1",
            },
        ),
    ]


@pytest.mark.parametrize("task", [{"task_id": "map-1"}, {"job_id": "job-1"}])
def test_process_map_task_requires_job_id_and_task_id(task: dict, tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        task_processing.process_map_task(task, make_task_paths(tmp_path), worker_id="worker-1")


def test_process_reduce_task_downloads_reduces_and_uploads_output(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    paths = make_task_paths(tmp_path)
    part_dir = tmp_path / "part_2"
    reduce_calls = []
    uploads = []

    class FakeReduceExecutor:
        def __init__(self, worker, sink, source):
            reduce_calls.append(("init", type(worker).__name__, sink.dirpath, type(source).__name__))

        def process(self, part_dir: str, part_num: int) -> None:
            reduce_calls.append(("process", part_dir, part_num))

    monkeypatch.setattr(task_processing, "download_part_files", lambda job_id, part_num, bucket: str(part_dir))
    monkeypatch.setattr(task_processing, "ReduceExecutor", FakeReduceExecutor)
    monkeypatch.setattr(
        task_processing,
        "upload_file",
        lambda local_path, bucket, key: uploads.append((local_path, bucket, key)),
    )

    task_processing.process_reduce_task(
        {"job_id": "job-1", "address": "2", "bucket": "bucket-1"},
        paths,
        worker_id="worker-1",
    )

    assert reduce_calls == [
        ("init", "WordCountReducer", paths.reduce_output_dir, "jsonDataSource"),
        ("process", str(part_dir), 2),
    ]
    assert uploads == [
        (
            os.path.join(paths.reduce_output_dir, "reduced_2.jsonl"),
            "bucket-1",
            reduce_output_key("job-1", 2),
        ),
    ]


def test_process_reduce_task_requires_job_id(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Missing job_id"):
        task_processing.process_reduce_task({"address": "0"}, make_task_paths(tmp_path), worker_id="worker-1")

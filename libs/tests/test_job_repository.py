import json

import pytest

import libs.job_repository.local_json as local_json_repository
from libs.job_repository import LocalJsonJobRepository


def test_local_json_repository_saves_and_loads_job(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    monkeypatch.setattr(local_json_repository.time, "time", lambda: 123.45)
    repository = LocalJsonJobRepository(job_dir=tmp_path / "jobs")

    job = repository.save(
        {
            "job_id": "job-1",
            "status": "uploaded",
            "original_filename": "\u0444\u0430\u0439\u043b.txt",
        }
    )

    assert job["updated_at"] == 123.45
    assert repository.load("job-1") == job
    assert json.loads((tmp_path / "jobs" / "job-1.json").read_text(encoding="utf-8")) == job


def test_local_json_repository_returns_none_for_missing_job(tmp_path) -> None:
    repository = LocalJsonJobRepository(job_dir=tmp_path / "jobs")

    assert repository.load("missing") is None


def test_local_json_repository_updates_existing_job(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    times = iter([100.0, 200.0])
    monkeypatch.setattr(local_json_repository.time, "time", lambda: next(times))
    repository = LocalJsonJobRepository(job_dir=tmp_path / "jobs")
    repository.save({"job_id": "job-1", "status": "uploaded", "chunk_count": 2})

    updated = repository.update(
        "job-1",
        {
            "status": "done",
            "result_key": "jobs/job-1/result/result.json",
        },
    )

    assert updated == {
        "job_id": "job-1",
        "status": "done",
        "chunk_count": 2,
        "updated_at": 200.0,
        "result_key": "jobs/job-1/result/result.json",
    }
    assert repository.load("job-1") == updated


def test_local_json_repository_returns_none_when_updating_missing_job(tmp_path) -> None:
    repository = LocalJsonJobRepository(job_dir=tmp_path / "jobs")

    assert repository.update("missing", {"status": "done"}) is None


def test_job_path_uses_repository_directory(tmp_path) -> None:
    repository = LocalJsonJobRepository(job_dir=tmp_path / "jobs")

    assert repository.job_path("job-1") == str(tmp_path / "jobs" / "job-1.json")

import json
import os
import time
from abc import ABC, abstractmethod
from pathlib import Path


DATA_DIR = "data"
JOB_DIR = os.path.join(DATA_DIR, "jobs")


class AbstractJobRepository(ABC):
    """Interface for storing job metadata."""

    @abstractmethod
    def save(self, job: dict) -> dict:
        pass

    @abstractmethod
    def load(self, job_id: str) -> dict | None:
        pass

    @abstractmethod
    def update(self, job_id: str, patch: dict) -> dict | None:
        pass


class LocalJsonJobRepository(AbstractJobRepository):
    """Stores job metadata as local JSON files."""

    def __init__(self, job_dir: str | Path = JOB_DIR):
        self.job_dir = str(job_dir)

    def _ensure_data_dirs(self) -> None:
        os.makedirs(self.job_dir, exist_ok=True)

    def job_path(self, job_id: str) -> str:
        return os.path.join(self.job_dir, f"{job_id}.json")

    def write_json_file(self, path: str, payload: dict) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)

        with open(path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)

    def read_json_file(self, path: str) -> dict | None:
        if not os.path.exists(path):
            return None

        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)

    def save(self, job: dict) -> dict:
        # A job is metadata for the whole client request, not the input file itself.
        self._ensure_data_dirs()
        job["updated_at"] = time.time()
        self.write_json_file(self.job_path(job["job_id"]), job)

        return job

    def load(self, job_id: str) -> dict | None:
        self._ensure_data_dirs()
        return self.read_json_file(self.job_path(job_id))

    def update(self, job_id: str, patch: dict) -> dict | None:
        job = self.load(job_id)
        if job is None:
            return None

        job.update(patch)
        job["updated_at"] = time.time()
        self.write_json_file(self.job_path(job_id), job)

        return job

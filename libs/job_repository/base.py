from abc import ABC, abstractmethod


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

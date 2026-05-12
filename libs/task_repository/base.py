from abc import ABC, abstractmethod

from libs.models import TaskCompletedEvent, WorkerTask


class AbstractTaskRepository(ABC):
    """Interface for storing worker task lifecycle metadata."""

    @abstractmethod
    def record_tasks_published(self, tasks: list[WorkerTask]) -> None:
        pass

    @abstractmethod
    def mark_task_completed(self, event: TaskCompletedEvent) -> None:
        pass

    @abstractmethod
    def mark_task_failed(self, task: dict, message: str | None = None) -> None:
        pass

    @abstractmethod
    def list_tasks_for_job(self, job_id: str) -> list[dict]:
        pass

    @abstractmethod
    def list_events_for_job(self, job_id: str) -> list[dict]:
        pass

from abc import ABC, abstractmethod

from libs.models import WorkerHeartbeat


class AbstractWorkerRepository(ABC):
    """Interface for storing worker liveness and current-task metadata."""

    @abstractmethod
    def record_heartbeat(self, heartbeat: WorkerHeartbeat) -> None:
        pass

    @abstractmethod
    def mark_workers_offline(self, cutoff_timestamp: float) -> int:
        pass

    @abstractmethod
    def list_workers(self) -> list[dict]:
        pass

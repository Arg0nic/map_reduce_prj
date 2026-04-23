from enum import StrEnum

from pydantic import BaseModel, Field


class TaskType(StrEnum):
    MAP = "map"
    REDUCE = "reduce"


class WorkerTask(BaseModel):
    job_id: str
    task_id: str
    type: TaskType
    address: str
    storage: str = "minio"
    bucket: str
    created_at: float
    part_num: int | None = Field(default=None, ge=0)

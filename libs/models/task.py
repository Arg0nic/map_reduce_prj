from enum import StrEnum

from pydantic import BaseModel, Field

from libs.storage_client.config import settings


DEFAULT_BUCKET = settings.DEFAULT_BUCKET or "mapreduce-data"


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


class TaskCompletedEvent(BaseModel):
    job_id: str
    task_id: str
    task_type: TaskType
    worker_id: str
    bucket: str = DEFAULT_BUCKET
    completed_at: float
    part_num: int | None = Field(default=None, ge=0)


class TaskOutputFile(BaseModel):
    part_num: int = Field(ge=0)
    key: str


class TaskOutputManifest(BaseModel):
    job_id: str
    task_id: str
    task_type: TaskType
    bucket: str = DEFAULT_BUCKET
    created_at: float
    outputs: list[TaskOutputFile]

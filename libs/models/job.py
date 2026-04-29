from enum import StrEnum

from pydantic import BaseModel, Field

from libs.storage_client.config import settings


DEFAULT_BUCKET = settings.DEFAULT_BUCKET or "mapreduce-data"


class JobStatus(StrEnum):
    UPLOADED = "uploaded"
    PROCESSING = "processing"
    DONE = "done"
    FAILED = "failed"


class ChunkInfo(BaseModel):
    part_index: int = Field(ge=0)
    key: str
    size_bytes: int = Field(ge=0)
    sha256: str


class JobUploadedEvent(BaseModel):
    job_id: str
    bucket: str = DEFAULT_BUCKET
    chunks_prefix: str
    created_at: float


class Job(BaseModel):
    job_id: str
    status: JobStatus
    original_filename: str
    storage: str = "minio"
    bucket: str
    chunk_count: int = Field(ge=0)
    total_bytes: int = Field(ge=0)
    chunks: list[ChunkInfo]
    submitted_at: float
    updated_at: float | None = None
    completed_at: float | None = None
    result_key: str | None = None
    planner_status: str | None = None
    planner_message: str | None = None

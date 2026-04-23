from typing import Any

from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str
    service: str


class UploadFileResponse(BaseModel):
    job_id: str


class NotReadyResponse(BaseModel):
    message: str = "Not ready yet"


class JobResultResponse(BaseModel):
    result: dict[str, Any]

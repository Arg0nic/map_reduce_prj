import json

from fastapi import FastAPI, HTTPException, UploadFile
from starlette.concurrency import run_in_threadpool

from api_gateway.schemas import HealthResponse, JobResultResponse, NotReadyResponse, UploadFileResponse
from api_gateway.service import JobService
from libs.models import JobStatus
from libs.storage_client.client import read_object_bytes


def create_app() -> FastAPI:
    app = FastAPI(title="MapReduce API Gateway")
    service: JobService | None = None

    def get_service() -> JobService:
        nonlocal service
        if service is None:
            service = JobService()
        return service

    @app.get("/health", response_model=HealthResponse)
    def health() -> dict:
        return {
            "status": "ok",
            "service": "api_gateway",
        }
    
    @app.post("/files", status_code=202, response_model=UploadFileResponse)
    async def upload_file(file: UploadFile) -> dict:
        try:
            active_service = get_service()
            job = await run_in_threadpool(
                active_service.create_from_upload,
                file.file,
                file.filename,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

        return {
            "job_id": job["job_id"],
        }
    
    @app.get("/jobs/{job_id}/result")
    def get_job_result(job_id: str):
        job = get_service().get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Job not found.")
        if job["status"] != JobStatus.DONE.value:
            return NotReadyResponse().model_dump()

        result_key = job.get("result_key")
        if not result_key:
            raise HTTPException(status_code=500, detail="Job is done but result key is missing.")

        try:
            result_bytes = read_object_bytes(job["bucket"], result_key)
            result = json.loads(result_bytes.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=500, detail="Job result is not valid JSON.") from exc
        except Exception as exc:
            raise HTTPException(status_code=503, detail="Failed to load job result.") from exc

        return JobResultResponse(result=result).model_dump()


    return app


app = create_app()

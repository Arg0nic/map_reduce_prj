import json
import logging

from fastapi import FastAPI, HTTPException, UploadFile
from starlette.concurrency import run_in_threadpool

from api_gateway.schemas import HealthResponse, JobResultResponse, NotReadyResponse, UploadFileResponse
from api_gateway.service import JobService
from libs.logging_config import configure_logging, format_log_fields
from libs.models import JobStatus
from libs.storage_client.client import read_object_bytes


logger = logging.getLogger(__name__)


def create_app() -> FastAPI:
    configure_logging("api_gateway")

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
        filename = file.filename
        logger.info("received file upload %s", format_log_fields(filename=filename))
        try:
            active_service = get_service()
            job = await run_in_threadpool(
                active_service.create_from_upload,
                file.file,
                filename,
            )
        except ValueError as exc:
            logger.warning(
                "rejected file upload %s",
                format_log_fields(filename=filename, error=str(exc)),
            )
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except RuntimeError as exc:
            logger.exception("failed to create job from upload %s", format_log_fields(filename=filename))
            raise HTTPException(status_code=503, detail=str(exc)) from exc

        logger.info(
            "accepted file upload %s",
            format_log_fields(job_id=job["job_id"], filename=filename),
        )
        return {
            "job_id": job["job_id"],
        }
    
    @app.get("/jobs/{job_id}/result")
    def get_job_result(job_id: str):
        job = get_service().get_job(job_id)
        if job is None:
            logger.info("job result requested for unknown job %s", format_log_fields(job_id=job_id))
            raise HTTPException(status_code=404, detail="Job not found.")
        if job["status"] != JobStatus.DONE.value:
            logger.info(
                "job result requested before completion %s",
                format_log_fields(job_id=job_id, status=job["status"]),
            )
            return NotReadyResponse().model_dump()

        result_key = job.get("result_key")
        if not result_key:
            logger.error("done job has no result key %s", format_log_fields(job_id=job_id))
            raise HTTPException(status_code=500, detail="Job is done but result key is missing.")

        try:
            result_bytes = read_object_bytes(job["bucket"], result_key)
            result = json.loads(result_bytes.decode("utf-8"))
        except json.JSONDecodeError as exc:
            logger.exception(
                "job result object is not valid JSON %s",
                format_log_fields(job_id=job_id, bucket=job["bucket"], result_key=result_key),
            )
            raise HTTPException(status_code=500, detail="Job result is not valid JSON.") from exc
        except Exception as exc:
            logger.exception(
                "failed to load job result object %s",
                format_log_fields(job_id=job_id, bucket=job["bucket"], result_key=result_key),
            )
            raise HTTPException(status_code=503, detail="Failed to load job result.") from exc

        logger.info(
            "returned job result %s",
            format_log_fields(job_id=job_id, bucket=job["bucket"], result_key=result_key),
        )
        return JobResultResponse(result=result).model_dump()


    return app


app = create_app()

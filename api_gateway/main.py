from fastapi import FastAPI, HTTPException, UploadFile
from starlette.concurrency import run_in_threadpool

from api_gateway.schemas import HealthResponse, NotReadyResponse, UploadFileResponse
from api_gateway.service import JobService
from libs.models import JobStatus


def create_app() -> FastAPI:
    app = FastAPI(title="MapReduce API Gateway")
    service = JobService()

    @app.get("/health", response_model=HealthResponse)
    def health() -> dict:
        return {
            "status": "ok",
            "service": "api_gateway",
        }
    
    @app.post("/files", status_code=202, response_model=UploadFileResponse)
    async def upload_file(file: UploadFile) -> dict:
        try:
            job = await run_in_threadpool(
                service.create_from_upload,
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
        job = service.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Job not found.")
        if job["status"] != JobStatus.DONE:
            return NotReadyResponse().model_dump()
        
        return 


    return app


app = create_app()

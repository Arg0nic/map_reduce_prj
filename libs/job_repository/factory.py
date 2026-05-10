import os

from dotenv import load_dotenv

from .base import AbstractJobRepository
from .local_json import LocalJsonJobRepository
from .postgres import PostgresJobRepository


load_dotenv()

DATABASE_URL_ENV = "DATABASE_URL"
JOB_REPOSITORY_BACKEND_ENV = "JOB_REPOSITORY_BACKEND"
JOB_REPOSITORY_BACKEND_POSTGRES = "postgres"
JOB_REPOSITORY_BACKEND_LOCAL_JSON = "local_json"


def create_job_repository(database_url: str | None = None) -> AbstractJobRepository:
    backend = os.getenv(JOB_REPOSITORY_BACKEND_ENV)

    if backend == JOB_REPOSITORY_BACKEND_POSTGRES:
        active_database_url = database_url if database_url is not None else os.getenv(DATABASE_URL_ENV)
        if not active_database_url:
            raise RuntimeError("DATABASE_URL is required when JOB_REPOSITORY_BACKEND=postgres.")
        return PostgresJobRepository(active_database_url)

    if backend == JOB_REPOSITORY_BACKEND_LOCAL_JSON:
        return LocalJsonJobRepository()

    raise RuntimeError(
        "JOB_REPOSITORY_BACKEND must be set to 'postgres' or 'local_json'."
    )

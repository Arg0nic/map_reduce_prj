import os

from dotenv import load_dotenv

from .base import AbstractWorkerRepository
from .postgres import PostgresWorkerRepository


load_dotenv()

DATABASE_URL_ENV = "DATABASE_URL"
WORKER_REPOSITORY_BACKEND_ENV = "WORKER_REPOSITORY_BACKEND"
WORKER_REPOSITORY_BACKEND_POSTGRES = "postgres"


def create_worker_repository(database_url: str | None = None) -> AbstractWorkerRepository:
    backend = os.getenv(WORKER_REPOSITORY_BACKEND_ENV, WORKER_REPOSITORY_BACKEND_POSTGRES)

    if backend == WORKER_REPOSITORY_BACKEND_POSTGRES:
        active_database_url = database_url if database_url is not None else os.getenv(DATABASE_URL_ENV)
        if not active_database_url:
            raise RuntimeError("DATABASE_URL is required when WORKER_REPOSITORY_BACKEND=postgres.")
        return PostgresWorkerRepository(active_database_url)

    raise RuntimeError("WORKER_REPOSITORY_BACKEND must be set to 'postgres'.")

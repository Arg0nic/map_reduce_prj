import os

from dotenv import load_dotenv

from .base import AbstractTaskRepository
from .postgres import PostgresTaskRepository


load_dotenv()

DATABASE_URL_ENV = "DATABASE_URL"
TASK_REPOSITORY_BACKEND_ENV = "TASK_REPOSITORY_BACKEND"
TASK_REPOSITORY_BACKEND_POSTGRES = "postgres"


def create_task_repository(database_url: str | None = None) -> AbstractTaskRepository:
    backend = os.getenv(TASK_REPOSITORY_BACKEND_ENV)

    if backend == TASK_REPOSITORY_BACKEND_POSTGRES:
        active_database_url = database_url if database_url is not None else os.getenv(DATABASE_URL_ENV)
        if not active_database_url:
            raise RuntimeError("DATABASE_URL is required when TASK_REPOSITORY_BACKEND=postgres.")
        return PostgresTaskRepository(active_database_url)

    raise RuntimeError("TASK_REPOSITORY_BACKEND must be set to 'postgres'.")

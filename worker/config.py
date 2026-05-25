from os import getenv
from pathlib import Path

from dotenv import load_dotenv


load_dotenv(Path(__file__).with_name(".env"))


def _int_env(name: str, default: int) -> int:
    value = getenv(name)
    if value is None:
        return default
    return int(value)


class WorkerSettings:
    RABBIT_LOGIN: str = getenv("RABBIT_LOGIN", "admin")
    RABBIT_PASS: str = getenv("RABBIT_PASS", "password")
    RABBIT_HOST: str = getenv("RABBIT_HOST", "localhost")
    RABBIT_PORT: int = _int_env("RABBIT_PORT", 5672)
    MAX_RETRIES: int = _int_env("WORKER_MAX_RETRIES", 3)


settings = WorkerSettings()

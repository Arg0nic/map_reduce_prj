from os import getenv
from pathlib import Path

from dotenv import load_dotenv


load_dotenv(Path(__file__).with_name(".env"))


def _int_env(name: str, default: int) -> int:
    value = getenv(name)
    if value is None:
        return default
    return int(value)


class PlannerSettings:
    RABBIT_LOGIN: str = getenv("RABBIT_LOGIN", "admin")
    RABBIT_PASS: str = getenv("RABBIT_PASS", "password")
    RABBIT_HOST: str = getenv("RABBIT_HOST", "localhost")
    RABBIT_PORT: int = _int_env("RABBIT_PORT", 5672)
    RUNNING_TASK_TIMEOUT_SECONDS: int = _int_env("RUNNING_TASK_TIMEOUT_SECONDS", 300)
    RUNNING_TASK_TIMEOUT_CHECK_SECONDS: int = _int_env("RUNNING_TASK_TIMEOUT_CHECK_SECONDS", 30)


settings = PlannerSettings()

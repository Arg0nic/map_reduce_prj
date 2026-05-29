from os import getenv
from pathlib import Path

from dotenv import load_dotenv


load_dotenv(Path(__file__).with_name(".env"))


def _int_env(name: str, default: int) -> int:
    value = getenv(name)
    if value is None:
        return default
    return int(value)


def _float_env(name: str, default: float) -> float:
    value = getenv(name)
    if value is None:
        return default
    return float(value)


class PlannerSettings:
    RABBIT_LOGIN: str = getenv("RABBIT_LOGIN", "admin")
    RABBIT_PASS: str = getenv("RABBIT_PASS", "password")
    RABBIT_HOST: str = getenv("RABBIT_HOST", "localhost")
    RABBIT_PORT: int = _int_env("RABBIT_PORT", 5672)
    RABBIT_CONNECT_RETRIES: int = _int_env("RABBIT_CONNECT_RETRIES", 30)
    RABBIT_CONNECT_RETRY_DELAY_SECONDS: float = _float_env("RABBIT_CONNECT_RETRY_DELAY_SECONDS", 2.0)
    RUNNING_TASK_TIMEOUT_SECONDS: int = _int_env("RUNNING_TASK_TIMEOUT_SECONDS", 300)
    RUNNING_TASK_TIMEOUT_CHECK_SECONDS: int = _int_env("RUNNING_TASK_TIMEOUT_CHECK_SECONDS", 30)


settings = PlannerSettings()

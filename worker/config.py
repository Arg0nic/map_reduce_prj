from pathlib import Path

from dotenv import load_dotenv

from libs.env import float_env, int_env, required_env


load_dotenv(Path(__file__).with_name(".env"))


class WorkerSettings:
    RABBIT_LOGIN: str = required_env("RABBIT_LOGIN")
    RABBIT_PASS: str = required_env("RABBIT_PASS")
    RABBIT_HOST: str = required_env("RABBIT_HOST")
    RABBIT_PORT: int = int_env("RABBIT_PORT", 5672)
    RABBIT_CONNECT_RETRIES: int = int_env("RABBIT_CONNECT_RETRIES", 30)
    RABBIT_CONNECT_RETRY_DELAY_SECONDS: float = float_env("RABBIT_CONNECT_RETRY_DELAY_SECONDS", 2.0)
    MAX_RETRIES: int = int_env("WORKER_MAX_RETRIES", 3)


settings = WorkerSettings()

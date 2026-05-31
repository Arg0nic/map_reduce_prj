from pathlib import Path

from dotenv import load_dotenv

from libs.env import float_env, int_env, required_env


load_dotenv(Path(__file__).with_name(".env"))


class PlannerSettings:
    RABBIT_LOGIN: str = required_env("RABBIT_LOGIN")
    RABBIT_PASS: str = required_env("RABBIT_PASS")
    RABBIT_HOST: str = required_env("RABBIT_HOST")
    RABBIT_PORT: int = int_env("RABBIT_PORT", 5672)
    RABBIT_CONNECT_RETRIES: int = int_env("RABBIT_CONNECT_RETRIES", 30)
    RABBIT_CONNECT_RETRY_DELAY_SECONDS: float = float_env("RABBIT_CONNECT_RETRY_DELAY_SECONDS", 2.0)
    RUNNING_TASK_TIMEOUT_SECONDS: int = int_env("RUNNING_TASK_TIMEOUT_SECONDS", 300)
    RUNNING_TASK_TIMEOUT_CHECK_SECONDS: int = int_env("RUNNING_TASK_TIMEOUT_CHECK_SECONDS", 30)


settings = PlannerSettings()

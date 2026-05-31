from pathlib import Path

from dotenv import load_dotenv

from libs.env import int_env, required_env


load_dotenv(Path(__file__).with_name(".env"))


class ApiGatewaySettings:
    RABBIT_LOGIN: str = required_env("RABBIT_LOGIN")
    RABBIT_PASS: str = required_env("RABBIT_PASS")
    RABBIT_HOST: str = required_env("RABBIT_HOST")
    RABBIT_PORT: int = int_env("RABBIT_PORT", 5672)


settings = ApiGatewaySettings()

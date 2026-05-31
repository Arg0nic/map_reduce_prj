from dotenv import load_dotenv

from libs.env import bool_env, required_env


load_dotenv()


class Settings:
    MINIO_ENDPOINT: str = required_env("MINIO_ENDPOINT")
    MINIO_ACCESS_KEY: str = required_env("MINIO_ACCESS_KEY")
    MINIO_SECRET_KEY: str = required_env("MINIO_SECRET_KEY")
    MINIO_SECURE: bool = bool_env("MINIO_SECURE", False)
    DEFAULT_BUCKET: str = required_env("MINIO_BUCKET")


settings = Settings()

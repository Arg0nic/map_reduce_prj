import os


TEST_ENV_DEFAULTS = {
    "RABBIT_HOST": "localhost",
    "RABBIT_LOGIN": "admin",
    "RABBIT_PASS": "password",
    "MINIO_ENDPOINT": "http://localhost:9000",
    "MINIO_ACCESS_KEY": "minioadmin",
    "MINIO_SECRET_KEY": "minioadmin",
    "MINIO_BUCKET": "mapreduce-data",
}


for name, value in TEST_ENV_DEFAULTS.items():
    os.environ.setdefault(name, value)

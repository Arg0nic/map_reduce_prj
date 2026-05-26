FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY libs/requirements.txt libs/requirements.txt
COPY worker/requirements.txt worker/requirements.txt
RUN pip install --no-cache-dir -r worker/requirements.txt

COPY libs/*.py libs/
COPY libs/models/*.py libs/models/
COPY libs/storage_client/*.py libs/storage_client/
COPY worker/*.py worker/

CMD ["python", "-m", "worker.main"]

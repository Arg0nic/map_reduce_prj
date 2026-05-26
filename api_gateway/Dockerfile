FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY libs/requirements.txt libs/requirements.txt
COPY api_gateway/requirements.txt api_gateway/requirements.txt
RUN pip install --no-cache-dir -r api_gateway/requirements.txt

COPY libs/*.py libs/
COPY libs/models/*.py libs/models/
COPY libs/storage_client/*.py libs/storage_client/
COPY libs/job_repository/*.py libs/job_repository/
COPY libs/task_repository/*.py libs/task_repository/
COPY api_gateway/*.py api_gateway/

EXPOSE 8000

CMD ["uvicorn", "api_gateway.main:app", "--host", "0.0.0.0", "--port", "8000"]

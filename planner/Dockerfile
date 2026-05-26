FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY libs/requirements.txt libs/requirements.txt
COPY planner/requirements.txt planner/requirements.txt
RUN pip install --no-cache-dir -r planner/requirements.txt

COPY libs/*.py libs/
COPY libs/models/*.py libs/models/
COPY libs/storage_client/*.py libs/storage_client/
COPY libs/job_repository/*.py libs/job_repository/
COPY libs/task_repository/*.py libs/task_repository/
COPY planner/*.py planner/

CMD ["python", "-m", "planner.main"]

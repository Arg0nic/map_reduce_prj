FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY libs/requirements.txt libs/requirements.txt
COPY planner/requirements.txt planner/requirements.txt
RUN pip install --no-cache-dir -r planner/requirements.txt

ENV PYTHONPATH=/app

COPY scripts/run_migrations.py scripts/run_migrations.py
COPY libs/*.py libs/
COPY migrations/*.sql migrations/

CMD ["python", "scripts/run_migrations.py"]

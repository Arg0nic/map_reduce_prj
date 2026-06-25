FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY libs/requirements.txt libs/requirements.txt
COPY scripts/requirements.txt scripts/requirements.txt
RUN pip install --no-cache-dir -r scripts/requirements.txt

ENV PYTHONPATH=/app

COPY alembic.ini alembic.ini
COPY libs/*.py libs/
COPY migrations/env.py migrations/env.py
COPY migrations/script.py.mako migrations/script.py.mako
COPY migrations/versions/ migrations/versions/

CMD ["alembic", "upgrade", "head"]

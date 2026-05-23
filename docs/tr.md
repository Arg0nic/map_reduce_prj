# Техническое описание проекта

## Назначение

Проект реализует распределенный pipeline для задачи word count по модели MapReduce. Система принимает текстовый файл, разбивает его на чанки, параллельно выполняет map/shuffle, затем reduce и сохраняет итоговый JSON-результат.

Документ описывает текущее состояние кода, а не первоначальный проектный план.

## Компоненты

### API Gateway

Код: `api_gateway/`.

Отвечает за HTTP-интерфейс:

- `POST /files` принимает файл, режет его на чанки и создает job;
- `GET /jobs/{job_id}/result` возвращает результат, если job уже завершен;
- `GET /health` возвращает health API Gateway.

API Gateway сохраняет job metadata через `JobRepository`, загружает чанки в MinIO и публикует `JobUploadedEvent` в RabbitMQ queue `jobs`.

### Planner

Код: `planner/`.

Отвечает за orchestration:

- читает `jobs`;
- создает map tasks;
- пишет task lifecycle в PostgreSQL;
- читает `task.completed`;
- читает `tasks.dead`;
- слушает `worker.heartbeat`;
- переводит running tasks в failed по timeout;
- запускает reduce phase после завершения всех map tasks;
- финализирует job после завершения всех reduce tasks.

Planner сейчас публикует все map tasks сразу. Controlled scheduling, при котором planner выдает tasks дозированно, пока не реализован.

### Worker

Код: `worker/`.

Worker является исполнителем task:

- читает tasks из RabbitMQ queue `tasks`;
- выполняет map или reduce;
- отправляет `task.completed`;
- при exception делает retry через RabbitMQ headers;
- после исчерпания retries отправляет task в `tasks.dead`;
- отправляет heartbeat в `worker.heartbeat`;
- во время выполнения task добавляет в heartbeat поле `current_task`.

Worker не принимает решений о lifecycle всего job. Если job уже failed, worker может завершить ранее полученную task, но planner проигнорирует late completion.

### PostgreSQL

Используется для metadata и task tracking.

Текущие таблицы:

- `jobs` - metadata job, chunks, status, result key, planner message;
- `tasks` - task lifecycle: published/running/completed/failed, worker_id, timestamps;
- `task_events` - история событий по tasks.

Время в PostgreSQL хранится как `TIMESTAMPTZ`, а наружу repository возвращает Unix timestamp float, чтобы не ломать текущий API/dict contract.

### RabbitMQ

Очереди:

- `jobs` - события от API Gateway к Planner;
- `tasks` - worker tasks;
- `task.completed` - completion events от worker к Planner;
- `tasks.dead` - tasks, которые исчерпали retries;
- `worker.heartbeat` - heartbeat events от worker.

### MinIO

Используется как object storage:

- input chunks;
- map outputs;
- map manifests;
- reduce outputs;
- reduce manifests;
- final result.

Reduce читает только outputs, на которые есть committed manifest. Это защищает reduce phase от partial uploads.

## Основной flow

1. Клиент отправляет файл в `POST /files`.
2. API Gateway режет файл на line-safe chunks. Размер чанка по умолчанию: `8 MB`.
3. Чанки сохраняются в MinIO под `jobs/{job_id}/chunks/`.
4. API Gateway создает job в repository и публикует `JobUploadedEvent`.
5. Planner читает event из `jobs`.
6. Planner создает один map task на каждый chunk и сразу публикует все map tasks в `tasks`.
7. Worker забирает map task, скачивает chunk, выполняет map и shuffle.
8. Worker загружает map outputs в MinIO и пишет map manifest.
9. Worker публикует `TaskCompletedEvent` в `task.completed`.
10. Planner отмечает map task completed.
11. Когда все map tasks job завершены, Planner читает map manifests и создает reduce tasks.
12. Worker выполняет reduce task: скачивает committed map outputs нужной partition, агрегирует данные, загружает reduce output и reduce manifest.
13. Planner после всех reduce completions собирает финальный результат, сохраняет result object в MinIO и обновляет job как `done`.
14. Клиент получает результат через `GET /jobs/{job_id}/result`.

## Количество tasks

Map phase:

```text
map tasks = количество input chunks
```

Чанки создаются API Gateway. По умолчанию один chunk примерно до `8 MB`, кроме случая, когда одна строка больше лимита.

Reduce phase:

```text
reduce tasks <= 4
```

Сейчас `WordCountShuffler` использует `num_parts=4`, поэтому reduce partitions максимум четыре.

Итого:

```text
tasks per job = chunk_count + up to 4 reduce tasks
```

Жесткого верхнего лимита на `chunk_count` в коде сейчас нет.

## Task lifecycle

Task statuses:

- `published` - Planner создал task и отправил ее в RabbitMQ;
- `running` - Planner увидел task в `current_task` внутри worker heartbeat;
- `completed` - Worker прислал `task.completed`;
- `failed` - task попала в dead queue или превысила running timeout.

Task events:

- `published`;
- `started`;
- `completed`;
- `dead_lettered`;
- `timed_out`.

## Heartbeat и timeout

Worker запускает отдельный heartbeat thread. Heartbeat отправляется в `worker.heartbeat`.

Если worker сейчас выполняет task, heartbeat содержит:

```json
{
  "worker_id": "worker-1",
  "ts": 123.45,
  "current_task": {
    "job_id": "job-1",
    "task_id": "map-1",
    "type": "map",
    "bucket": "mapreduce-data",
    "started_at": 120.0,
    "part_num": null
  }
}
```

Planner по такому heartbeat переводит task из `published` в `running`, записывает `started_at` и `worker_id`.

Planner также запускает timeout monitor thread. Он периодически ищет tasks со статусом `running`, у которых `started_at` старше допустимого timeout.

Текущие значения в коде:

```text
RUNNING_TASK_TIMEOUT_SECONDS = 300
RUNNING_TASK_TIMEOUT_CHECK_SECONDS = 30
```

Если task timed out:

- task становится `failed`;
- job становится `failed`;
- поздние `task.completed` для этого job игнорируются.

## Retry и dead queue

Если worker ловит exception во время обработки task:

1. Увеличивает RabbitMQ header `x-attempts`.
2. Если attempts меньше `MAX_RETRIES`, публикует task обратно в `tasks`.
3. Если attempts достиг лимита, публикует task в `tasks.dead`.

Текущий лимит:

```text
MAX_RETRIES = 3
```

Planner читает `tasks.dead`, помечает task failed и весь job failed.

## Guarantees and limitations

Что уже есть:

- durable RabbitMQ messages для jobs/tasks/completions;
- retries worker tasks при exception;
- dead queue после исчерпания retries;
- PostgreSQL metadata для jobs/tasks/events;
- heartbeat с `current_task`;
- timeout running tasks;
- late completion после failed job игнорируется;
- manifests защищают reduce от partial map outputs.

Что пока осознанно не реализовано:

- нет retry/reschedule для timed out tasks;
- нет attempt_id, поэтому retry attempts не разделены как отдельные сущности;
- worker не отменяет task во время выполнения, если job уже failed;
- уже опубликованные tasks failed job могут доработать впустую;
- нет автоматической очистки мусора в MinIO после failed job;
- нет worker registry таблицы;
- нет отдельного Scheduler/Worker Manager сервиса;
- Planner публикует все map tasks сразу.

Текущая политика:

```text
task dead/timed out -> job failed
worker может завершить уже полученные tasks
planner игнорирует late completions для failed/done job
автоматический cleanup пока не выполняется
```

## Storage layout

Основные prefixes в MinIO:

```text
jobs/{job_id}/chunks/
jobs/{job_id}/map_outputs/{map_task_id}/
jobs/{job_id}/map_manifests/{map_task_id}.json
jobs/{job_id}/reduce_outputs/{reduce_task_id}/
jobs/{job_id}/reduce_manifests/{reduce_task_id}.json
jobs/{job_id}/result/
```

Map output и reduce output считаются committed только после записи manifest.

## Database migrations

SQL migrations находятся в `migrations/`.

Текущие файлы:

- `001_create_jobs.sql`;
- `002_create_task_tracking.sql`.

Запуск:

```powershell
python scripts/run_migrations.py
```

Для запуска нужен `DATABASE_URL` в `.env`.

Пример:

```env
DATABASE_URL=postgresql+psycopg://mapreduce:mapreduce_password@localhost:5432/mapreduce
JOB_REPOSITORY_BACKEND=postgres
TASK_REPOSITORY_BACKEND=postgres
```

Если схема менялась, а локальные данные не нужны, можно удалить таблицы и прогнать migrations заново:

```sql
DROP TABLE IF EXISTS task_events, tasks, jobs CASCADE;
```

Потом:

```powershell
python scripts/run_migrations.py
```

## Local run commands

API Gateway:

```powershell
python -m uvicorn api_gateway.main:app --reload --port 8000
```

Swagger:

```text
http://localhost:8000/docs
```

Planner:

```powershell
python planner/main.py
```

Worker:

```powershell
python worker/main.py
```

Tests:

```powershell
pytest -q
```

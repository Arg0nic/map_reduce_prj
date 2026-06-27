# Техническое описание проекта

## Назначение

Проект реализует распределенный пайплайн для задачи Word Count по модели MapReduce. Система принимает текстовый файл, разбивает его на чанки, параллельно выполняет map/shuffle, затем reduce и сохраняет итоговый JSON-результат.

Этот документ описывает текущее состояние проекта, а не первоначальный план разработки.

## Основные компоненты

### API Gateway

Код находится в `api_gateway/`.

API Gateway отвечает за HTTP-интерфейс системы:

- `POST /files` принимает файл, разбивает его на чанки и создает `job`;
- `GET /jobs/{job_id}/result` возвращает результат, если `job` уже завершен;
- `GET /health` возвращает состояние API Gateway.

После загрузки файла API Gateway сохраняет метаданные через `JobRepository`, загружает чанки в MinIO и публикует событие `JobUploadedEvent` в RabbitMQ-очередь `jobs`.

### Planner

Код находится в `planner/`.

Planner отвечает за оркестрацию всего пайплайна:

- читает события из очереди `jobs`;
- создает map tasks;
- записывает жизненный цикл tasks в PostgreSQL;
- читает события из очереди `task.completed`;
- читает задачи из очереди `tasks.dead`;
- слушает heartbeat-сообщения из очереди `worker.heartbeat`;
- переводит зависшие `running` tasks в `failed` по таймауту;
- запускает reduce-этап после завершения всех map tasks;
- финализирует `job` после завершения всех reduce tasks.

Сейчас Planner публикует все map tasks сразу. Более аккуратное управляемое планирование, при котором Planner выдает задачи дозированно, пока не реализовано.

### Worker

Код находится в `worker/`.

Worker исполняет отдельные tasks:

- читает задачи из RabbitMQ-очереди `tasks`;
- выполняет map или reduce;
- отправляет событие `task.completed`;
- при ошибке делает повторную попытку через RabbitMQ headers;
- после исчерпания повторных попыток отправляет task в `tasks.dead`;
- отправляет heartbeat в `worker.heartbeat`;
- во время выполнения задачи добавляет в heartbeat поле `current_task`.

Worker не принимает решений о состоянии всего `job`. Если `job` уже переведен в `failed`, worker может завершить ранее полученную task, но Planner проигнорирует такое позднее событие завершения.

### PostgreSQL

PostgreSQL используется для хранения метаданных и отслеживания tasks.

Текущие таблицы:

- `jobs` - метаданные job, список чанков, статус, ключ результата и сообщение Planner;
- `tasks` - жизненный цикл task: `published`, `running`, `completed`, `failed`, `worker_id`, временные метки;
- `task_events` - история событий по tasks.

Время в PostgreSQL хранится как `TIMESTAMPTZ`. Наружу repository возвращает Unix timestamp в формате `float`, чтобы не ломать текущий контракт API и словарей.

### RabbitMQ

RabbitMQ используется как брокер сообщений между сервисами.

Основные очереди:

- `jobs` - события от API Gateway к Planner;
- `tasks` - задачи для Worker;
- `task.completed` - события о завершении task от Worker к Planner;
- `tasks.dead` - задачи, которые исчерпали повторные попытки;
- `worker.heartbeat` - heartbeat-сообщения от Worker.

### MinIO

MinIO используется как объектное хранилище.

В MinIO хранятся:

- входные чанки;
- map outputs;
- map manifests;
- reduce outputs;
- reduce manifests;
- финальный результат.

Reduce читает только те outputs, для которых есть подтвержденный manifest. Это защищает reduce-этап от чтения частично загруженных данных.

## Основной сценарий работы

1. Клиент отправляет файл в `POST /files`.
2. API Gateway разбивает файл на чанки по границам строк. Размер чанка по умолчанию: `8 MB`.
3. Чанки сохраняются в MinIO под префиксом `jobs/{job_id}/chunks/`.
4. API Gateway создает `job` в repository и публикует `JobUploadedEvent`.
5. Planner читает событие из очереди `jobs`.
6. Planner создает один map task на каждый chunk и публикует все map tasks в очередь `tasks`.
7. Worker забирает map task, скачивает chunk, выполняет map и shuffle.
8. Worker загружает map outputs в MinIO и записывает map manifest.
9. Worker публикует `TaskCompletedEvent` в очередь `task.completed`.
10. Planner отмечает map task как `completed`.
11. Когда все map tasks завершены, Planner читает map manifests и создает reduce tasks.
12. Worker выполняет reduce task: скачивает подтвержденные map outputs нужной partition, агрегирует данные, загружает reduce output и reduce manifest.
13. Когда все reduce tasks завершены, Planner собирает финальный результат, сохраняет объект результата в MinIO и переводит `job` в `done`.
14. Клиент получает результат через `GET /jobs/{job_id}/result`.

## Количество задач

Map-этап:

```text
map tasks = количество входных чанков
```

Чанки создает API Gateway. По умолчанию один chunk имеет размер примерно до `8 MB`, кроме случая, когда одна строка больше лимита.

Reduce-этап:

```text
reduce tasks <= 4
```

Сейчас `WordCountShuffler` использует `num_parts=4`, поэтому reduce partitions максимум четыре.

Итоговая оценка:

```text
tasks per job = chunk_count + up to 4 reduce tasks
```

Жесткого верхнего лимита на `chunk_count` в коде сейчас нет.

## Жизненный цикл task

Статусы task:

- `published` - Planner создал task и отправил ее в RabbitMQ;
- `running` - Planner увидел task в `current_task` внутри worker heartbeat;
- `completed` - Worker прислал `task.completed`;
- `failed` - task попала в dead queue или превысила таймаут выполнения.

События task:

- `published`;
- `started`;
- `completed`;
- `dead_lettered`;
- `timed_out`.

## Heartbeat и таймауты

Worker запускает отдельный heartbeat thread. Heartbeat отправляется в очередь `worker.heartbeat`.

Если worker сейчас выполняет task, heartbeat содержит поле `current_task`:

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

Planner также запускает отдельный thread для контроля таймаутов. Он периодически ищет tasks со статусом `running`, у которых `started_at` старше допустимого таймаута.

Текущие значения:

```text
RUNNING_TASK_TIMEOUT_SECONDS = 300
RUNNING_TASK_TIMEOUT_CHECK_SECONDS = 30
```

Если task превысила таймаут:

- task переводится в `failed`;
- весь `job` переводится в `failed`;
- поздние `task.completed` для этого `job` игнорируются.

Heartbeat-очередь объявляется с TTL для сообщений. Старые heartbeat-сообщения после рестарта Planner не несут полезной информации, поэтому очередь очищается при старте Planner.

## Повторные попытки и dead queue

Если Worker ловит exception во время обработки task:

1. Worker увеличивает RabbitMQ header `x-attempts`.
2. Если количество attempts меньше `MAX_RETRIES`, task публикуется обратно в очередь `tasks`.
3. Если количество attempts достигло лимита, task публикуется в очередь `tasks.dead`.

Текущий лимит:

```text
MAX_RETRIES = 3
```

Planner читает `tasks.dead`, помечает task как `failed` и переводит весь `job` в `failed`.

## Гарантии и ограничения

Что уже реализовано:

- durable RabbitMQ messages для jobs, tasks и completion-событий;
- повторные попытки для worker tasks при exception;
- dead queue после исчерпания повторных попыток;
- метаданные в PostgreSQL для jobs, tasks и events;
- heartbeat с полем `current_task`;
- таймаут для зависших `running` tasks;
- игнорирование поздних completion-событий после `failed` или `done` job;
- manifests, защищающие reduce от частично загруженных map outputs;
- Alembic migrations для схемы PostgreSQL.

Что пока не реализовано:

- нет повторного запуска или reschedule для timed out tasks;
- нет `attempt_id`, поэтому retry attempts не разделены как отдельные сущности;
- Worker не отменяет task во время выполнения, если `job` уже стал `failed`;
- уже опубликованные tasks для failed job могут доработать впустую;
- нет автоматической очистки мусора в MinIO после failed job;
- нет worker registry таблицы;
- нет отдельного Scheduler/Worker Manager сервиса;
- Planner публикует все map tasks сразу.

Текущая политика отказов:

```text
task dead/timed out -> job failed
worker может завершить уже полученные tasks
planner игнорирует поздние completion-события для failed/done job
автоматический cleanup пока не выполняется
```

## Структура объектов в MinIO

Основные префиксы:

```text
jobs/{job_id}/chunks/
jobs/{job_id}/map_outputs/{map_task_id}/
jobs/{job_id}/map_manifests/{map_task_id}.json
jobs/{job_id}/reduce_outputs/{reduce_task_id}/
jobs/{job_id}/reduce_manifests/{reduce_task_id}.json
jobs/{job_id}/result/
```

Map output и reduce output считаются подтвержденными только после записи соответствующего manifest.

## Миграции базы данных

Миграции PostgreSQL управляются через Alembic.

Текущая схема описана SQLAlchemy ORM-моделями в `libs/db_models.py`; Alembic использует их metadata для autogenerate.

Текущие ревизии:

- `migrations/versions/001_create_jobs.py`;
- `migrations/versions/002_create_task_tracking.py`.

Запуск локально:

```powershell
uv run alembic upgrade head
```

Для запуска нужен `DATABASE_URL` в `.env`.

Пример:

```env
DATABASE_URL=postgresql+psycopg://mapreduce:mapreduce_password@localhost:5432/mapreduce
JOB_REPOSITORY_BACKEND=postgres
TASK_REPOSITORY_BACKEND=postgres
```

В Docker Compose миграции запускаются автоматически через service `migrate`. Остальные сервисы, которым нужна база данных, ждут успешного завершения `migrate`.

Если схема менялась, а локальные данные не нужны, можно удалить таблицы и прогнать миграции заново:

```sql
DROP TABLE IF EXISTS alembic_version, task_events, tasks, jobs CASCADE;
```

После этого:

```powershell
uv run alembic upgrade head
```

## Локальный запуск

API Gateway:

```powershell
uv run python -m uvicorn api_gateway.main:app --reload --port 8000
```

Swagger UI:

```text
http://localhost:8000/docs
```

Planner:

```powershell
uv run python planner/main.py
```

Worker:

```powershell
uv run python worker/main.py
```

Тесты:

```powershell
uv run pytest -q
```

Docker Compose:

```powershell
docker compose up --build
```

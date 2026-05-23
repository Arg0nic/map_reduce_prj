# MapReduce word count

Актуальная документация проекта.

- [Техническое описание](./tr.md)

## Коротко

Проект реализует асинхронный MapReduce pipeline для подсчета слов:

1. API Gateway принимает текстовый файл.
2. Файл режется на чанки и сохраняется в MinIO.
3. Planner создает map tasks и отправляет их в RabbitMQ.
4. Worker выполняет map/shuffle, сохраняет committed outputs и manifest в MinIO.
5. Planner после завершения map phase создает reduce tasks.
6. Worker выполняет reduce и сохраняет reduce manifest.
7. Planner собирает финальный результат и обновляет job в PostgreSQL.

Текущее состояние проекта: рабочий pipeline с PostgreSQL metadata, RabbitMQ orchestration, MinIO object storage, heartbeat tracking, dead queue handling и timeout для running tasks.

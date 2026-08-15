# tasks

`tasks` — внутренний сервис Overmindv для создания, публикации и решения IT-задач. Он поддерживает `single_choice`, `multiple_choice` и асинхронную проверку `programming`-задач через Kafka, неизменяемые версии условий и историю пользовательских решений.

## Возможности

- административный CRUD тестов с soft delete;
- lifecycle `draft -> published -> archived -> draft`;
- новая неизменяемая версия при каждом обновлении;
- проверка ответа по версии, которую видел пользователь;
- идемпотентные отправки и история результатов;
- загрузка одного UTF-8 файла решения на Python или Go;
- transactional outbox для отправки запуска и inbox для дедупликации результата sandbox;
- необязательная связь версии с `topic_id` из `entities`;
- очередь собранных кандидатов с optimistic locking, approve/reject и неизменяемой атрибуцией источника;
- защищённый bearer token'ом batch ingestion API для `task-hunter`;
- внутренний HTTP/JSON API без GraphQL и выполнения пользовательского кода внутри `tasks`.

Правильные варианты доступны только административному API и пользователю после сохранения решения. `tasks` не хранит профили пользователей и не обращается к БД других сервисов.

Для `programming` options всегда пусты, а choice submission возвращает `TASK_TYPE_NOT_SUBMITTABLE`. Условие, теги, открытые тесты из `examples`, ограничения и источник хранятся на уровне неизменяемой версии. Файловый submit доступен отдельно по `POST /v1/tasks/{id}/code-submissions`.

`tasks` не запускает пользовательский код. Он публикует `code_execution.requested` в `code-execution.requests.v1`, принимает `code_execution.completed` из `code-execution.results.v1` и отдаёт состояние через polling HTTP API. Реализация `sandbox` находится вне этого репозитория и этой задачи.

## Локальный запуск

Требуются Go 1.26.1, PostgreSQL 17+ и Kafka, доступная по адресу из `KAFKA_BOOTSTRAP_SERVERS`.

```bash
# 1) один раз: конфигурация из .env.example
cp .env.example .env

# 2) убедитесь, что PostgreSQL запущен и БД tasks создана
#    (как создать БД — см. docs/development.md)

# 3) миграции + запуск сервиса одной командой
make dev
```

Значения из `.env` подхватываются `make` автоматически. `make dev` сначала применяет миграции, затем запускает сервис; раздельный вариант — `make migrate-up` и `make run`. Проверка готовности PostgreSQL и Kafka доступна по `GET /ready`, liveness — по `GET /health`.

## Проверки

```bash
make test
go vet ./...
make lint
make ctest COMPONENT_TEST_DSN='postgres://postgres:postgres@localhost:5432/tasks?sslmode=disable'
docker build -t tasks:local .
```

`make ctest` применяет миграции к выделенной тестовой БД и запускает полный HTTP-сценарий. Не направляйте эту команду на общую или production database: тест очищает таблицы `tasks`.

## Разработка

Пошаговый гайд «как добавить новый HTTP-эндпоинт» и детали локального запуска: [`docs/development.md`](docs/development.md).

## Контракты

- HTTP API: [`api/openapi.yaml`](api/openapi.yaml);
- Kafka request/result JSON Schema: каталог [`api/events`](api/events);
- архитектура: [`docs/architecture.md`](docs/architecture.md);
- будущие изменения связанных сервисов: каталог [`docs/integrations`](docs/integrations).

Административные и пользовательские методы доверяют заголовкам `X-User-ID` и `X-User-Roles`, которые может выставлять только `api-gateway` во внутренней сети.

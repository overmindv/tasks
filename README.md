# tasks-it

`tasks-it` — внутренний сервис Overmindv для создания, публикации и решения IT-тестов. Первый MVP поддерживает `single_choice` и `multiple_choice`, неизменяемые версии условий и историю пользовательских решений.

## Возможности

- административный CRUD тестов с soft delete;
- lifecycle `draft -> published -> archived -> draft`;
- новая неизменяемая версия при каждом обновлении;
- проверка ответа по версии, которую видел пользователь;
- идемпотентные отправки и история результатов;
- необязательная связь версии с `topic_id` из `entities`;
- внутренний HTTP/JSON API без GraphQL и выполнения пользовательского кода.

Правильные варианты доступны только административному API и пользователю после сохранения решения. `tasks-it` не хранит профили пользователей и не обращается к БД других сервисов.

## Локальный запуск

Требуются Go 1.26.1 и PostgreSQL 17+.

```bash
cp .env.example .env
make migrate-up
set -a && source .env && set +a
make run
```

Миграции выполняются отдельно до запуска процесса. Проверка готовности доступна по `GET /ready`, liveness — по `GET /health`.

## Проверки

```bash
make test
go vet ./...
make lint
make ctest COMPONENT_TEST_DSN='postgres://postgres:postgres@localhost:5432/tasks_it?sslmode=disable'
docker build -t tasks-it:local .
```

`make ctest` применяет миграции к выделенной тестовой БД и запускает полный HTTP-сценарий. Не направляйте эту команду на общую или production database: тест очищает таблицы `tasks-it`.

## Контракты

- HTTP API: [`api/openapi.yaml`](api/openapi.yaml);
- архитектура: [`docs/architecture.md`](docs/architecture.md);
- будущие изменения связанных сервисов: каталог [`docs/integrations`](docs/integrations).

Административные и пользовательские методы доверяют заголовкам `X-User-ID` и `X-User-Roles`, которые может выставлять только `api-gateway` во внутренней сети.

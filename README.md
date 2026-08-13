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
# 1) один раз: конфигурация из .env.example
cp .env.example .env

# 2) убедитесь, что PostgreSQL запущен и БД tasks_it создана
#    (как создать БД — см. docs/development.md)

# 3) миграции + запуск сервиса одной командой
make dev
```

Значения из `.env` подхватываются `make` автоматически; ручной `source .env` не нужен. `make dev` сначала применяет миграции, затем запускает сервис. Раздельно: `make migrate-up`, затем `make run`.

Альтернатива без `.env` — передать переменные окружения (`make run DATABASE_URL='…'` или экспортировать их в shell).

Проверка готовности — `GET /ready`, liveness — `GET /health`. Подробный гайд по разработке и добавлению эндпоинтов: [`docs/development.md`](docs/development.md).

## Проверки

```bash
make test
go vet ./...
make lint
make ctest COMPONENT_TEST_DSN='postgres://postgres:postgres@localhost:5432/tasks_it?sslmode=disable'
docker build -t tasks-it:local .
```

`make ctest` применяет миграции к выделенной тестовой БД и запускает полный HTTP-сценарий. Не направляйте эту команду на общую или production database: тест очищает таблицы `tasks-it`.

## Разработка

Пошаговый гайд «как добавить новый HTTP-эндпоинт» и детали локального запуска: [`docs/development.md`](docs/development.md).

## Контракты

- HTTP API: [`api/openapi.yaml`](api/openapi.yaml);
- архитектура: [`docs/architecture.md`](docs/architecture.md);
- будущие изменения связанных сервисов: каталог [`docs/integrations`](docs/integrations).

Административные и пользовательские методы доверяют заголовкам `X-User-ID` и `X-User-Roles`, которые может выставлять только `api-gateway` во внутренней сети.

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository

`tasks-it` — внутренний Go-сервис Overmindv для IT-тестов: административный CRUD задач, неизменяемое версионирование условий, публикация и проверка пользовательских решений. Первый MVP поддерживает `single_choice` и `multiple_choice`. Это сервис внутренней сети, не публикуется наружу.

**Важно:** проект имеет подробный `AGENTS.md` (правила слоёв, версионирование, безопасность, чекеры, миграции, Go-конвенции). Он обязателен к прочтению перед изменениями границ сервиса и при нетривиальных задачах — не дублируется здесь.

## Команды

Требования: Go 1.26.1, PostgreSQL 17+.

```bash
cp .env.example .env                              # один раз; .env подхватывается make автоматически
make dev                                          # goose-миграции + запуск сервиса одной командой
make run                                          # только запуск (go run ./cmd/tasks-it)
make migrate-up                                   # только миграции, если БД уже поднята

make build                                        # go build ./...
make test                                         # go test -race ./...
go vet ./...
make lint                                         # golangci-lint run (gofmt/goimports/staticcheck и др.)
go test ./<package>/...                           # тесты одного пакета

# component-тесты (применяют миграции к ВЫДЕЛЕННОЙ тестовой БД, очищают таблицы tasks-it)
make ctest COMPONENT_TEST_DSN='postgres://postgres:postgres@localhost:5432/tasks_it?sslmode=disable'
```

`make jet-generate` перегенерирует типизированные Jet-модели в `internal/adapter/postgres/generated` из live-schema БД (нужен поднятый PostgreSQL). `.env` и БД-соединения — в `.gitingore`, в репозиторий не коммитятся.

Проверка: `GET /health` (liveness), `GET /ready` (гот. PostgreSQL).

## Архитектура

Чистая гексагональная слоёность; поток: transport → usecase → repository interface → postgres adapter.

- `cmd/tasks-it` — wiring (config, pgxpool, логгер, graceful shutdown) и точка входа.
- `internal/domain` — типы и **инварианты** (`task.go`, `submission.go`): lifecycle `draft → published → archived → draft`, версионирование, лимиты полей, валидация вариантов по типу теста. Бизнес-инварианты проверяются здесь/usecase, не в transport.
- `internal/checker` — детерминированное сравнение ответов (`Choice`: множества UUID), без состояния.
- `internal/usecase` — оркестрация сценариев и транзакции. `TaskService` (CRUD, status, версии), `SubmissionService` (проверка прав, идемпотентность, запись результата одной транзакцией). Boundary транзакций — здесь (`Repository.WithinTransaction`).
- `internal/repository` — persistence-интерфейс (`Repository`), независимый от реализации.
- `internal/adapter/postgres` — pgx + типизированные **Jet expressions**; `schema.go` — структуры таблиц. Transport/adapter не вырабатывают доменные решения.
- `internal/transport/httpapi` — DTO, Внутренний HTTP-роутинг (std `net/http`, Go 1.22 method-паттерны), `middleware.go` (request ID, безопасный access/log, recover). Transport DTO ≠ доменные модели.
- `internal/apperror` — публичные/внутренние ошибки; внутр. детали мапятся в transport, SQL-детали не утекают.
- `internal/config` — загрузка/валидация env на старте.

## Ключевые модели данных

- `Task` (lifecycle, `CurrentVersionID`) + неизменяемые `TaskVersion` (содержимое, `Options []TaskOption`). Обновление атомарно создаёт новую версию и переключает `current_version_id`.
- `Submission` всегда хранит точный `TaskVersionID` (проверка по версии, которую видел пользователь). Грейс-сценарий: после обновления теста результат помечается `TaskUpdated=true` и несёт актуальную версию; проверка идёт по старой.
- Идемпотентность: повтор с тем же `idempotency_key` возвращает прежний результат; изменение payload → конфликт.

## Безопасность и границы

- Сервис доверяет заголовкам `X-User-ID` и `X-User-Roles` **только** от внутреннего `api-gateway`; admin = роли `admin`/`superuser` (см. `http.go: requireUser/requireAdmin`).
- До submit public API не возвращает `is_correct`. Request-логи не содержат body, выбранные ответы или правильные варианты.
- Внешние UUID хранятся как opaque IDs **без** межсервисных foreign keys; нет копий записей пользователей/тем.
- Hidden tests / reference answers / checker секреты никогда не возвращаются публичными эндпоинтами.
- Все SQL — параметризованные Jet-выражения; без конкатенации пользовательского ввода.
- Пока нет запуска пользовательского кода; если появляется — за интерфейсом `internal/execution/` и с песочницей (см. `AGENTS.md`).

## Контракты и документация

- HTTP API: `api/openapi.yaml`.
- Архитектура: `docs/architecture.md`; изменения связанных сервисов: `docs/integrations/`.
- Миграции — append-only в `migrations/`, не менять применённые.
- Смена контракта: проверить производителей/потребителей, обратную совместимость, обновить сгенерированный код и контракт-тесты.

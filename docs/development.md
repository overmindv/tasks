# Разработка tasks-it

Гайд для разработчиков: локальный запуск и добавление новых HTTP-эндпоинтов.

## Локальный запуск

Требуются **Go 1.26.1** и **PostgreSQL 17+**.

### 1. Конфигурация

Скопируйте шаблон конфигурации (один раз):

```bash
cp .env.example .env
```

`.env` подхватывается `make` автоматически — ручной `source .env` не нужен. Все переменные с
ключами из среды (`DATABASE_URL`, `HTTP_ADDR`, `LOG_LEVEL`, …) доступны и как переменные
окружения, и для make-таргетов.

### 2. База данных

Убедитесь, что PostgreSQL запущен, и создайте БД, если её ещё нет:

```bash
createdb tasks_it
```

Если вы используете другой пользователь/хост/имя, укажите их в `DATABASE_URL` в `.env`
(формат `postgres://user:password@host:5432/tasks_it?sslmode=disable`).

### 3. Запуск

```bash
make dev        # миграции + запуск сервиса одной командой
```

Отдельные шаги: `make migrate-up` (только миграции) и `make run` (только запуск).
Проверки: `GET /health` (liveness), `GET /ready` (готовность PostgreSQL).
Логи пишутся в STDOUT в формате JSON.

### 4. Проверки

```bash
make build                          # компиляция всего проекта
make test                           # unit/use-case тесты (-race)
go vet ./...
make lint                           # golangci-lint (gofmt/goimports/staticcheck)
make ctest COMPONENT_TEST_DSN='…'   # component-тесты (нужна отдельная тестовая БД)
```

## Как добавить новый HTTP-эндпоинт

Проект — чистая гексагональная слоёность. Запрос идёт по цепочке:

```
request → transport/httpapi (DTO + хэндлер)
        → usecase (сценарий, транзакции)
        → repository (интерфейс хранения)
        → adapter/postgres (pgx + Jet)
```

Новый эндпоинт обычно затрагивает 4 слоя + тесты. Ниже — порядок шагов и куда что класть.

### Шаг 1. DTO запроса/ответа — `internal/transport/httpapi/dto.go`

Добавьте DTO запроса (из JSON) и ответа (в JSON). Пример:

```go
type hintRequest struct {
	TaskID  string `json:"task_id"`
	Version int    `json:"version"`
}

type hintResponse struct {
	TaskID string `json:"task_id"`
	Text   string `json:"text"`
}
```

Если DTO нужно преобразовать в доменный/use-case тип, добавьте метод рядом (как `domainInput`,
`domainSubmissionInput`).

### Шаг 2. Хэндлер + регистрация маршрута — `internal/transport/httpapi/`

Хэндлер разместите в файле по домену: задачи — `tasks.go`, решения — `submissions.go`.
Типовой шаблон: проверка роли → разбор `{id}` → декод тела → вызов use-case → запись ответа.

```go
// getTaskHints возвращает подсказки задачи для владельца. Ответ: 200 + подсказки.
func (h *Handler) getTaskHints(w http.ResponseWriter, r *http.Request) {
	actor, ok := requireUser(w, r) // requireUser — для пользователя; requireAdmin — для админа
	if !ok {
		return
	}
	taskID, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	result, err := h.tasks.Hints(r.Context(), taskID, actor.UserID)
	if err != nil {
		writeError(w, err, h.logger)
		return
	}
	writeJSON(w, http.StatusOK, responseHints(result))
}
```

Зарегистрируйте маршрут в `handler.go` в функции `New()` — в подходящей секции по назначению
(health / admin tasks / published tasks / submissions):

```go
// Пользовательские решения.
mux.HandleFunc("POST /v1/tasks/{id}/submissions", handler.submitAnswer)
mux.HandleFunc("GET /v1/submissions/{id}", handler.getSubmission)
mux.HandleFunc("GET /v1/me/submissions", handler.listMySubmissions)
mux.HandleFunc("GET /v1/tasks/{id}/hints", handler.getTaskHints) // новый маршрут
```

Замечания:

- метод и path задаются в строке паттерна (`GET /v1/tasks/{id}/hints`), параметр — через `{id}`;
- доступ к параметру — `r.PathValue("id")` (см. `pathUUID`);
- тело декодируйте через готовый хелпер (`decodeTaskInput`, `decodeSubmissionInput`) либо
  `decodeJSON`; ответ — через `writeJSON`/`writeError`;
- «простые» общие помощники (`requireUser`, `requireAdmin`, `pathUUID`, `decodeJSON`,
  `pagination`, `writeJSON`, `writeError`) лежат в `http.go`, не дублируйте их.

### Шаг 3. Use-case — `internal/usecase/*.go`

Добавьте метод сервиса (`TaskService`/`SubmissionService`): здесь живут сценарии, проверка прав
и границы транзакций (`repository.WithinTransaction`). Валидация инвариантов — в `internal/domain`:

```go
// Hints возвращает подсказки задачи, доступные владельцу.
func (s *TaskService) Hints(ctx context.Context, taskID, userID uuid.UUID) ([]string, error) {
	// ... сценарий: доступ, чтение данных репозитория, сборка результата.
}
```

### Шаг 4. Интерфейс хранения — `internal/repository/repository.go`

Добавьте сигнатуру метода в интерфейс `Repository`:

```go
GetTaskHints(ctx context.Context, taskID uuid.UUID) ([]domain.TaskHint, error)
```

### Шаг 5. Реализация хранения — `internal/adapter/postgres/*.go`

Реализуйте метод на `*Postgres` (pgx + типизированные Jet-выражения), например в `tasks.go`.
Все запросы — параметризованные Jet-выражения; без конкатенации пользовательского ввода.
Если появляются новые таблицы/колонки — добавьте структуру в `schema.go` и миграцию
в `migrations/` (append-only), затем `make jet-generate`, если используете сгенерированные модели.

### Шаг 6. Тесты

- unit/use-case: по образцу `internal/usecase/usecase_test.go` и `internal/domain/task_test.go`;
- transport/доступ: `internal/transport/httpapi/handler_test.go` (скрытие `is_correct`, роли);
- полный сценарий: добавьте шаги в `tests/component/task_flow_test.go` (тест с тегом `component`).

### Шаг 7. Контракт и документация

- API-контракт: `api/openapi.yaml`;
- при изменениях межсервисных интерфейсов — `docs/architecture.md` и `docs/integrations/`.

## Частые непонятные места

- **Куда класть хэндлер?** По домену задачи — `tasks.go`, решения — `submissions.go`.
  Если домен новый — создайте файл `internal/transport/httpapi/<domain>.go` и добавьте обзорный
  комментарий в его начало.
- **Нужна ли новая таблица?** Да, если данных нет в `tasks`/`task_versions`/`task_options`
  (`submissions`/`submission_answers` для решений). Добавьте миграцию и структуру в `schema.go`.
- **Где валидировать?** Инварианты и бизнес-правила — `internal/domain`/`usecase`;
  в transport — только распаковка DTO и формат (типы, UUID).

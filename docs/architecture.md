# Архитектура tasks-it

## Граница сервиса

`tasks-it` владеет тестовыми задачами, их версиями, вариантами ответа и пользовательскими решениями. Пользователи принадлежат `users`, канонические темы — `entities`, GraphQL API — `api-gateway`, представление — `frontend`.

Внешние UUID сохраняются как opaque identifiers без межсервисных foreign keys. В MVP `topic_id` может отсутствовать и не проверяется синхронно.

## Слои

- `internal/domain` — типы, lifecycle и инварианты тестов;
- `internal/checker` — детерминированное сравнение ответов;
- `internal/usecase` — транзакционные сценарии и авторизация результата;
- `internal/repository` — storage contract;
- `internal/adapter/postgres` — типизированные Jet-запросы поверх pgx;
- `internal/transport/httpapi` — DTO, HTTP routing, trusted actor boundary;
- `cmd/tasks-it` — wiring, сигналы и lifecycle процесса.

Transport DTO не используются как доменные или PostgreSQL-модели. Все SQL-операции строятся Jet expressions, входные значения параметризуются.

## Версионирование

`tasks` хранит status, аудит и ссылку на текущую версию. `task_versions` и `task_options` после создания не изменяются. Обновление теста атомарно создаёт новую версию, новые option IDs и переключает `current_version_id`.

`submission` всегда содержит точный `task_version_id`. Если пользователь отправляет ответ после обновления теста, checker использует старые варианты, а результат получает `task_updated=true` и актуальную версию.

## Решение теста

1. `api-gateway` проверяет JWT через `users` и передаёт доверенный `X-User-ID`.
2. Use case проверяет, что агрегат опубликован и версия принадлежит задаче.
3. Выбранные option IDs проверяются относительно указанной версии.
4. Checker сравнивает множества выбранных и правильных UUID.
5. `submission` и `submission_answers` сохраняются одной транзакцией.
6. Повтор с тем же `idempotency_key` возвращает прежний результат; изменение payload даёт конфликт.

Архивирование и soft delete запрещают новые решения, но не удаляют историю.

## Безопасность

Сервис не должен публиковаться напрямую наружу. Роли принимаются только от внутреннего `api-gateway`; admin-операции разрешены `admin` и `superuser`. До submit public API никогда не возвращает `is_correct`.

Request logs не содержат body, выбранные ответы или правильные варианты. Внутренние SQL-ошибки преобразуются в общий `INTERNAL_ERROR`.

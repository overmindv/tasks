# Архитектура tasks

## Граница сервиса

`tasks` владеет тестовыми задачами, их версиями, вариантами ответа и пользовательскими решениями, включая состояние асинхронных запусков кода. Пользователи принадлежат `users`, канонические темы — `entities`, GraphQL API — `api-gateway`, представление — `frontend`, безопасное выполнение кода — `sandbox`.

Пользователи принадлежат `users`, канонические темы — `entities`, GraphQL API — `api-gateway`, представление — `frontend`.

Внешние UUID сохраняются как opaque identifiers без межсервисных foreign keys.

## Слои

- `internal/domain` — типы, lifecycle и инварианты тестов;
- `internal/checker` — детерминированное сравнение ответов;
- `internal/usecase` — транзакционные сценарии и авторизация результата;
- `internal/repository` — storage contract;
- `internal/adapter/postgres` — типизированные Jet-запросы поверх pgx;
- `internal/transport/httpapi` — DTO, HTTP routing, trusted actor boundary;
- `internal/execution` — versioned Kafka request/result contracts;
- `internal/transport/kafka` — outbox dispatcher и result consumer;
- `cmd/tasks` — wiring, сигналы и lifecycle процесса.

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

## Решение programming-задачи

1. Пользователь загружает ровно один UTF-8 файл `.py` или `.go` размером до 256 KiB и передаёт UUID idempotency key.
2. Use case проверяет опубликованную programming-версию и формирует тесты только из открытых `examples`, у которых задан ожидаемый output.
3. `code_submissions` и `code_submission_outbox` записываются одной PostgreSQL-транзакцией. HTTP сразу возвращает `202` и статус `queued`.
4. Dispatcher публикует request в Kafka topic `code-execution.requests.v1` с ключом `execution_id`. После broker acknowledgement payload outbox-сообщения очищается; исходный код решения в `code_submissions` намеренно сохраняется (возвращается в API детали запуска своей страницы).
5. `sandbox` независимо читает request topic, безопасно запускает код и публикует результат в `code-execution.results.v1`. Реализация sandbox не входит в `tasks`.
6. Consumer строго проверяет JSON и совпадение `submission_id`, `execution_id` и `task_id` с сохранённым запуском. Inbox record и финальный результат сохраняются одной транзакцией, затем Kafka offset подтверждается.
7. Вердикт вычисляется самим `tasks` из вывода кейсов (sandbox возвращает только вывод и метаданные). Финальные verdict'ы — в allowlist `domain.IsFinalExecutionVerdict`; `infrastructure_error`/`checker_error` никогда не помечают решение как неверное. Инфраструктурные ошибки переотправляются до лимита `MaxAttempts`, затем завершаются как `infrastructure_error`; заброшенные `queued`-запуски переотправляет reaper. Клиент опрашивает `GET /v1/code-submissions/{id}`: статусы `queued` и `completed`.

Доставка request events — at-least-once: после Kafka ack и до PostgreSQL mark возможен повтор. Стабильный `execution_id` позволяет sandbox дедуплицировать запуск. Result events дедуплицируются одновременно по Kafka topic/partition/offset и `event_id`. Невалидные сообщения записываются в inbox без payload и подтверждаются, чтобы не создавать poison-message loop.

## Безопасность

Сервис не должен публиковаться напрямую наружу. Роли принимаются только от внутреннего `api-gateway`; admin-операции разрешены `admin` и `superuser`. До submit public API никогда не возвращает `is_correct`.

Request logs не содержат body, исходный код, выбранные ответы или правильные варианты. HTTP API результата не возвращает исходный код. Kafka logs содержат только event/submission ID и topic metadata. Внутренние SQL-ошибки преобразуются в общий `INTERNAL_ERROR`.

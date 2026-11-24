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
4. Dispatcher публикует request в Kafka topic `code-execution.requests.v1` с ключом `submission_id`. После broker acknowledgement payload outbox-сообщения очищается; исходный код решения в `code_submissions` намеренно сохраняется (возвращается в API детали запуска своей страницы).
5. `sandbox` независимо читает request topic, безопасно запускает код и публикует результат в `code-execution.results.v1`. Реализация sandbox не входит в `tasks`.
6. Consumer строго проверяет JSON и совпадение `submission_id`, `execution_id` и `task_id` с сохранённым запуском. Inbox record и финальный результат сохраняются одной транзакцией, затем Kafka offset подтверждается.
7. Вердикт вычисляется самим `tasks` из вывода кейсов (sandbox возвращает только вывод и метаданные). Финальные verdict'ы — в allowlist `domain.IsFinalExecutionVerdict`; `infrastructure_error`/`checker_error` никогда не помечают решение как неверное. Инфраструктурные ошибки переотправляются до лимита `MaxAttempts`, затем завершаются как `infrastructure_error`; заброшенные `queued`-запуски переотправляет reaper. Клиент опрашивает `GET /v1/code-submissions/{id}`: статусы `queued` и `completed`.

Доставка request events — at-least-once: после Kafka ack и до PostgreSQL mark возможен повтор. Стабильный `attempt_id` внутри доставки позволяет sandbox дедуплицировать запуск; новый retry tasks получает новый ID. Result events дедуплицируются одновременно по Kafka topic/partition/offset и `event_id`. Невалидные сообщения записываются в inbox без payload и подтверждаются, чтобы не создавать poison-message loop.

Producer и results consumer используют общие TLS/SASL-настройки parker (`KAFKA_TLS_*`, `KAFKA_SASL_*`): в production TLS обязателен, SASL — только поверх TLS, конфигурация проверяется до подключения к брокеру.

### Повторная обработка результатов (OVM-255)

Ошибка сохранения результата удерживает текущую Kafka-запись: следующего poll
до успешного commit нет. После успешного сохранения повторяется только Kafka
commit. Backoff составляет 1–5 секунд, общий бюджет записи — 30 секунд при
rebalance timeout 60 секунд. При исчерпании бюджета consumer возвращает ошибку,
освобождает rebalance, а runnable закрывает клиент; дальнейший запуск должен
создать новый клиент. Неподтверждённая запись доставляется повторно, inbox
сохраняет идемпотентность. Это retry доставки результата, а не новый запуск кода.
При длительной недоступности БД ошибка runnable завершает сервис; эксплуатация
должна обеспечивать перезапуск процесса.

Проверка реального брокера на отдельном тестовом Kafka (создаёт временные topics):
`TEST_KAFKA_BROKERS=127.0.0.1:39092 go test -race -tags=integration ./internal/transport/kafka`.
Тест инъецирует ошибку хранилища на границе use case; PostgreSQL он не заменяет.

## Лента активности (Feed)

Публикация задачи пишет business-событие в `task.activity.v1` через parker-outbox
в транзакции смены статуса. Для пересборки ленты после retention Kafka сервис
дополнительно ведёт durable-журнал `task_activity_changes`: триггер на `tasks`
фиксирует публикацию и снятие с публикации в порядке коммитов. Журнал отдаётся
внутренним потребителям страницами на закреплённом watermark:
`GET /v1/internal/task-activity/changes`. Контракт — [task-activity.md](task-activity.md).

## Безопасность

Сервис не должен публиковаться напрямую наружу. Роли принимаются только от внутреннего `api-gateway`; admin-операции разрешены `admin` и `superuser`. До submit public API никогда не возвращает `is_correct`.

Request logs не содержат body, исходный код, выбранные ответы или правильные варианты. HTTP API результата не возвращает исходный код. Kafka logs содержат только event/submission ID и topic metadata. Внутренние SQL-ошибки преобразуются в общий `INTERNAL_ERROR`.

Точный wire-контракт и ограничения текущего retry: [execution-contract](execution-contract.md).

### Изоляция попыток (OVM-245)

`code_submissions.execution_id` — ID текущей попытки, `attempts` — её номер.
При retry ID и номер меняются атомарно с outbox; `submission_id`/версия остаются.
Reaper и result consumer берут row lock, повторно проверяют текущий ID и срок.
`next_attempt_at` — общий deadline текущей попытки: первичное значение и каждое
новое окно равны now + RetryGrace. Результат после срока обрабатывается как timeout,
независимо от его created_at; исчерпание MaxAttempts даёт infrastructure_error.
Результаты старой/завершённой попытки сохраняются в inbox со status=stale без payload.
Ошибки БД остаются retryable, кроме подтверждённого отсутствия submission/version.

Перед выкладкой применить 202611150001_execution_attempt_fencing.sql. Она расширяет
inbox status constraint. Для перехода остановить старые workers/reaper: смешанный
rollout небезопасен, поскольку старый код не соблюдает fencing. Старые queued
записи с истёкшим окном могут получить новый ID; их поздние результаты станут stale.
История старых attempt ID отдельной таблицей не хранится. Defaults внутренних
callers без policy — 3 попытки и 2 минуты; production использует валидируемый config.

### Приватные наборы и режимы (OVM-242/243/244)

Административный draft с CAS revision публикуется как новая TaskVersion и immutable
suite binding. S3 получает только stdin; эталоны и checker хранятся в tasks.
Samples проверяет examples и не участвует в прогрессе/рейтинге. Graded использует
suite закреплённой версии; hidden stdout/stderr не сохраняются. Миграция трактует
исторические code submissions как samples. Контракт, rollout и ограничения:
[OVM-244](changes/OVM-244.md), [публикация suite](changes/OVM-243.md).

### Списки задач (OVM-251)

`problem_lists` создаются идемпотентно по `source_url` при импорте кандидатов из
`task-hunter`. Признак `truncated` отражает частичный сбор; `problem_list_position`
фиксирует порядок сбора и переносится в `problem_list_members` при одобрении.
Пользовательская выдача (`/v1/problem-lists`) показывает только опубликованные
задачи, административная (`/v1/admin/problem-lists`) — черновики и метаданные
модерации. Детали и контракт: [problem-lists](problem-lists.md).

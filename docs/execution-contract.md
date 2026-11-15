# Контракт tasks ↔ sandbox (OVM-241)

## Wire v1 и владельцы

| Producer | Topic | Consumer | Schema |
| --- | --- | --- | --- |
| tasks outbox | code-execution.requests.v1 | sandbox | sandbox.run.request.v1 |
| sandbox | code-execution.retry.v1 | sandbox | тот же request + retry headers |
| sandbox | code-execution.results.v1 | tasks inbox | sandbox.run.result.v1 |

Канонические JSON Schema — sandbox/api/schemas. Tasks/api/events содержит точные
snapshots и проверяет отсутствие дрейфа. Старые code_execution.requested/completed
не используются в этих topics. Execution results не являются публичными activity
или авторитетными учебными outcome events: последние определяются отдельно
в DOC-03/OVM-126 и OVM-249. Эти задачи не закрыты данным изменением.

## Идентичность и версия

- Kafka key новых запросов, sandbox retries/results — строка submission_id.
- submission_id — неизменяемое решение пользователя; attempt_id — конкретное исполнение.
- task_id указывает задачу; metadata.task_version_id в запросе tasks указывает
  неизменяемую версию из code_submissions.task_version_id.
- task_version_id не добавляется top-level в v1: строгие старые consumers отвергнут
  неизвестное поле. Existing metadata поддерживает scalar значения.
- OVM-246: новый tasks задаёт metadata.result_context_version="1". Только тогда
  sandbox возвращает evaluation_context: task_version_id и suite_sha256.
  Checksum вычисляется sandbox по фактическому упорядоченному execution.inputs
  (SHA-256 от Go json.Marshal массива строк). Версия связывает входы с эталонами.
  Это проверка корреляции доверенного runner, не аттестация его исполнения.
- trace_id — существующий correlation ID; отдельный event_id в v1 не вводится.

## Режимы и профиль tasks

Execution: execution.mode=execution, inputs — stdin-кейсы. Sandbox возвращает
status=ok и cases; tasks сравнивает stdout с expected в своей БД.
Pytest/go-test: tests.mode, inline files или object_storage reference; sandbox
возвращает passed/failed/compile_error и tests/summary. Schemas и общие fixtures
охватывают все три режима, но tasks пока создаёт и оценивает только execution.
Приём test-mode envelope декодером tasks не означает поддержку grading test-mode.

Sandbox допускает 32 stdin-кейса и до 1 MiB вывода. Профиль текущего tasks уже:
не более 20 кейсов, 64 KiB stdout/stderr на кейс, error.message до 2000 байт.
Схема описывает sandbox envelope; use-case policy/validators tasks добавляют
собственные ограничения. JSON Schema maxLength считает Unicode code points,
а runtime-лимиты размера stdin/output — UTF-8 байты; для многобайтных строк
схема не заменяет runtime validation. OVM-246 проверяет полную матрицу: число/уникальность/границы кейсов,
summary, run-level failure, signal/exit code и усечение вывода.
Неполный ok и несовпадение контекста запускают bounded retry, не accepted.
Checker/config failure отделён от wrong_answer и infrastructure_error.
Нельзя расширять output limits tasks только потому, что это допускает sandbox.

## Владельцы retry

Нормативная политика:

1. Outbox tasks повторяет доставку того же payload/attempt_id до broker ack.
2. Sandbox самостоятельно повторяет инфраструктурный сбой внутри attempt_id:
   тот же payload, bounded retry headers/backoff, дедупликация в storage.
3. После terminal infrastructure result новый запуск инициирует только tasks:
   новый уникальный attempt_id, прежние submission_id и task_version_id.
4. Повтор результата доставляется at-least-once; tasks inbox идемпотентен.
   Повтор Kafka commit результата (OVM-255) не создаёт новое исполнение.
5. Поздний результат предыдущей попытки не должен завершать текущую попытку.
   Reaper не должен конкурировать с ещё действующим бюджетом sandbox retry.

**Реализовано OVM-245:** tasks атомарно меняет execution_id/attempt_id при retry
и сохраняет новый request в outbox. Reaper и result consumer сериализуют переход
по строке решения; completion и retry защищены CAS текущего execution_id.
Поздние ответы записываются как stale inbox. Оба обработчика используют общий
next_attempt_at текущей попытки и MaxAttempts; просроченный ответ не финализирует
решение, а запускает тот же retry-or-exhausted переход, что reaper.
RetryGrace должен покрывать очередь, выполнение и внутренние retries sandbox;
новый глобальный SLA и удалённая отмена старого sandbox job не добавлены.

Sandbox при исчерпании retry публикует известный terminal result или
internal_error и DLQ; attempt_in_progress — исключение: активное исполнение не
завершается фиктивным результатом. DLQ не является учебным вердиктом.

## Совместимость и интеграция

Изменение schemas отражает уже существующий runtime execution-mode; номер v1
сохраняется. Metadata добавляется совместимо. Новые обязательные поля, изменение
смысла status/ID либо неизвестные строгому decoder поля требуют согласованного
v2 topic и миграции producer/consumer. Даже optional top-level поле нельзя считать
совместимым без проверки строгих Go decoder.

Обновить schemas/tests sandbox и snapshots/tests tasks, затем producer tasks.
Старые outbox записи не переписываются: они сохраняют ключ execution_id и могут
оказаться в другой partition. Нельзя полагаться на порядок между старым и новым
ключом; inbox и OVM-245 остаются обязательными. Push/deployment не выполняется.

## Проверки

В обоих репозиториях: `go test ./internal/...`, JSON Schema через
`scripts/check_execution_contract.py`; `--peer ../sandbox` (для tasks) или
`--peer ../tasks` (для sandbox) проверяет равенство schemas и fixtures.
CI каждого репозитория проверяет его snapshot независимо от sibling checkout.
Тесты не исполняют пользовательский код и не требуют Kafka/gojudge.

## Развёртывание расширения OVM-246

Сначала обновить sandbox (старые запросы не получают нового поля), затем tasks.
Tasks требует контекст версии/checksum для оценки. Старые queued результаты без
контекста приводят к bounded retry с новым opt-in запросом, без изменения старого
outbox. При исчерпании попыток — infrastructure_error. Смешанный rollout старых
и новых tasks consumers небезопасен: старый строгий decoder не знает расширение.
Перед переключением остановить старые consumers; rollback требует drain новых
result сообщений. HTTP/GraphQL и таблицы БД не меняются.
Текущий suite содержит только открытые примеры. Hidden grading/sample separation
не добавлены; OVM-244 остаётся отдельной задачей.

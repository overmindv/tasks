# План задач: tasks

Дата анализа: **31 октября 2026**. Локальный HEAD: `4195ddc`; анализ также учитывает рабочее дерево на 2026-10-31.

## Текущее состояние

Есть choice-проверка, immutable task versions, programming-submit, inbox/outbox, retry/reaper и история. Programming запускает открытые examples; режима полноценной проверки hidden suite ещё нет. Подборки problem_lists уже добавлены в незакоммиченной работе — завершить и проверить интеграцию.

Проверенные опорные файлы: `internal/usecase/code_submissions.go`, `internal/execution/contracts.go`, `internal/domain/task.go`, `internal/domain/problem_list.go`, `migrations/202610100001_add_problem_lists.sql`, `docs/architecture.md`.

## Правила выполнения

- Оценка по договорённости: **1 SP ≈ 1 рабочий день; 2 SP ≈ 2 рабочих дня одного разработчика**, включая проверку и review. Это предварительная оценка, не гарантия календарного срока.
- Сначала реализовать весь описанный в карточке функциональный срез, затем написать/обновить тесты последним шагом. Без TDD; общий e2e — после интеграции. Уже существующие проверки не выключать.
- Статус всех карточек: **запланировано**, код по этому backlog ещё не изменён. Наличие исходников и тестов не означает подтверждённую работоспособность.
- Обязательные зависимости указаны явно; рабочая очередь и расчёт срока — в [общем плане](../tasks.md). F0–F4 — технические этапы, а PILOT/PROD/LATER — границы выпуска.
- При смене контракта: согласовать производителя и потребителей, сгенерировать код, обеспечить совместимый rollout. Миграции только append-only; секреты/hidden tests/исходники решений в обычные логи не писать.
- Задачу больше двух дней после уточнения делить на отдельно принимаемые части, сохраняя ID родителя как ссылку. Не скрывать дополнительный объём внутри пункта «и всё остальное».

Всего: **15 задач / 26 SP**; обязательный срез: **11 задач / 19 SP**.

<a id="tsk-01"></a>

## [tasks] Align the execution contract with sandbox

- **ID:** TSK-01
- **Оценка:** 1 SP (1 рабочий день)
- **Этап:** F0 — решения и контракты; **контур:** PILOT — обязательный срез.
- **Приоритет:** P0 — блокер корректности/безопасности.
- **Зависит от:** [DOC-03](../docs/tasks.md#doc-03).
- **Где менять:** `internal/execution/contracts.go`, `api/events`, `../sandbox/api/schemas`.
- **Готово, когда:** Есть один согласованный контракт без двух конкурирующих retry-политик.

**Шаги:**

1. Сверить реальные JSON schemas и fixtures producer/consumer, включая stdin-case и pytest/go-test режимы.
2. Зафиксировать key=submission_id, уникальный attempt_id и связь с task_version_id.
3. Определить владелец retry: sandbox внутри attempt, tasks создаёт новый attempt; для несовместимых изменений новый topic version.
4. **Проверка после готовности функционала:** schema compatibility проверки обеих сторон на одинаковых fixtures.

<a id="tsk-02"></a>

## [tasks] Version private test suites

- **ID:** TSK-02
- **Оценка:** 2 SP (2 рабочих дня)
- **Этап:** F1 — надёжная основа; **контур:** PILOT — обязательный срез.
- **Приоритет:** P1 — обязательная функция.
- **Зависит от:** [TSK-01](#tsk-01).
- **Где менять:** `internal/domain/task.go`, `migrations`, `internal/adapter/postgres`.
- **Готово, когда:** Историческое решение сохраняет неизменный набор проверки.

**Шаги:**

1. Добавить immutable suite metadata и тесты public/hidden, привязанные к TaskVersion.
2. Ограничить размер, число кейсов, checksum и checker allowlist.
3. Изменение suite/лимитов/языка создаёт новую версию.
4. **Проверка после готовности функционала:** unit invariants и component создания новой версии.

<a id="tsk-03"></a>

## [tasks] Add administrative test suite publishing

- **ID:** TSK-03
- **Оценка:** 2 SP (2 рабочих дня)
- **Этап:** F1 — надёжная основа; **контур:** PILOT — обязательный срез.
- **Приоритет:** P1 — обязательная функция.
- **Зависит от:** [TSK-02](#tsk-02).
- **Где менять:** `internal/transport/httpapi`, `internal/usecase`, `api/openapi.yaml`.
- **Готово, когда:** Опубликованный manifest нельзя заменить; публичные DTO не содержат private ссылок.

**Шаги:**

1. Добавить admin-only CRUD черновика suite и публикацию private immutable manifest в object storage.
2. Записывать suite_id/version/checksum только после успешной загрузки, orphan чистить отдельно.
3. Проверять пригодность suite и наличие hidden tests до полного submit.
4. **Проверка после готовности функционала:** ACL, повтор publish и сбой storage до commit.

<a id="tsk-04"></a>

## [tasks] Separate sample runs from graded submissions

- **ID:** TSK-04
- **Оценка:** 2 SP (2 рабочих дня)
- **Этап:** F1 — надёжная основа; **контур:** PILOT — обязательный срез.
- **Приоритет:** P1 — обязательная функция.
- **Зависит от:** [TSK-03](#tsk-03).
- **Где менять:** `internal/usecase/code_submissions.go`, `internal/domain/code_submission.go`.
- **Готово, когда:** Два режима проверяют разные наборы и имеют раздельную семантику результата.

**Шаги:**

1. Добавить run_mode=samples/graded в запись и idempotency hash.
2. Для samples использовать открытые examples, для graded закреплённый suite reference.
3. Передать новые поля через REST, schema и outbox; samples не дают освоение темы.
4. **Проверка после готовности функционала:** unit/usecase на режимы, отсутствие suite и конфликт idempotency key.

<a id="tsk-05"></a>

## [tasks] Fence retries by execution attempt

- **ID:** TSK-05
- **Оценка:** 2 SP (2 рабочих дня)
- **Этап:** F1 — надёжная основа; **контур:** PILOT — обязательный срез.
- **Приоритет:** P0 — блокер корректности/безопасности.
- **Зависит от:** [TSK-01](#tsk-01).
- **Где менять:** `internal/usecase/code_submissions.go`, `internal/adapter/postgres/execution_queue.go`.
- **Готово, когда:** Поздний ответ старого запуска не завершает новый; число попыток ограничено.

**Шаги:**

1. Хранить отдельные attempt_id и номера попыток; retry создаёт новый ID.
2. Финализировать только текущую попытку через CAS, поздние результаты сохранять как stale inbox.
3. Reaper и result consumer используют один лимит, общий deadline и одинаковые переходы.
4. **Проверка после готовности функционала:** concurrent reaper/result, duplicate и out-of-order component.

<a id="tsk-06"></a>

## [tasks] Validate complete execution results

- **ID:** TSK-06
- **Оценка:** 2 SP (2 рабочих дня)
- **Этап:** F1 — надёжная основа; **контур:** PILOT — обязательный срез.
- **Приоритет:** P0 — блокер корректности/безопасности.
- **Зависит от:** [TSK-04](#tsk-04), [TSK-05](#tsk-05).
- **Где менять:** `internal/usecase/code_submissions.go`, `internal/execution/contracts.go`.
- **Готово, когда:** Неполный либо подменённый result не становится успешным решением.

**Шаги:**

1. Сверить ID/версию/checksum suite, число и уникальность кейсов.
2. Запретить accepted при missing/duplicate/unknown cases и учитывать run-level failure.
3. Разделить неправильный ответ, checker_error и infrastructure_error.
4. **Проверка после готовности функционала:** regression на частичные cases, чужой attempt и run-level error.

<a id="tsk-07"></a>

## [tasks] Redact hidden test execution output

- **ID:** TSK-07
- **Оценка:** 2 SP (2 рабочих дня)
- **Этап:** F1 — надёжная основа; **контур:** PILOT — обязательный срез.
- **Приоритет:** P0 — блокер корректности/безопасности.
- **Зависит от:** [TSK-06](#tsk-06).
- **Где менять:** `internal/transport/httpapi/code_submissions.go`, `internal/usecase`.
- **Готово, когда:** Код, печатающий вход hidden test, не раскрывает его через API или логи.

**Шаги:**

1. Для hidden вернуть только агрегатный verdict и разрешённые метрики.
2. Убрать stdout/stderr, входы, expected, artifact URLs и checker details из public DTO.
3. Сохранить подробности только в изолированном диагностическом канале с retention.
4. **Проверка после готовности функционала:** security fixtures echo-input, stderr leak, history/list и admin boundary.

<a id="tsk-08"></a>

## [tasks] Apply submission admission limits

- **ID:** TSK-08
- **Оценка:** 1 SP (1 рабочий день)
- **Этап:** F1 — надёжная основа; **контур:** PILOT — обязательный срез.
- **Приоритет:** P1 — обязательная функция.
- **Зависит от:** [TSK-04](#tsk-04).
- **Где менять:** `internal/usecase/code_submissions.go`, `migrations`.
- **Готово, когда:** Повторные клики и параллельные запросы не обходят квоту.

**Шаги:**

1. Ограничить concurrent queued/active per user и общий backlog.
2. Проверять квоту и создавать submission атомарно, повтор idempotency не расходует квоту.
3. Возвращать RATE_LIMITED и безопасное время повтора.
4. **Проверка после готовности функционала:** concurrent requests, terminal release и idempotency тесты.

<a id="tsk-09"></a>

## [tasks] Publish authoritative learning outcomes

- **ID:** TSK-09
- **Оценка:** 2 SP (2 рабочих дня)
- **Этап:** F2 — пользовательские функции; **контур:** PILOT — обязательный срез.
- **Приоритет:** P1 — обязательная функция.
- **Зависит от:** [TSK-06](#tsk-06), [TSK-07](#tsk-07), [TSK-15](#tsk-15), [DOC-03](../docs/tasks.md#doc-03).
- **Где менять:** `internal/usecase/submissions.go`, `internal/usecase/code_submissions.go`, `migrations`.
- **Готово, когда:** Recommendations получает достоверный результат ровно один раз по смыслу после дедупликации.

**Шаги:**

1. Добавить learning.outcome.v1 отдельно от activity topic.
2. Сохранять outcome outbox в транзакции финального graded результата для choice/programming.
3. Передавать user/task/version/topics/verdict/graded_at, признак первой независимой попытки и correction_version; исключить код и тесты.
4. **Проверка после готовности функционала:** rollback, duplicate completion, samples и infrastructure_error.

<a id="tsk-10"></a>

## [tasks] Expose learning resource metadata

- **ID:** TSK-10
- **Оценка:** 2 SP (2 рабочих дня)
- **Этап:** F2 — пользовательские функции; **контур:** PILOT — обязательный срез.
- **Приоритет:** P1 — обязательная функция.
- **Зависит от:** [ENT-04](../entities/tasks.md#ent-04), [DOC-03](../docs/tasks.md#doc-03).
- **Где менять:** `internal/transport/httpapi`, `internal/usecase/tasks.go`.
- **Готово, когда:** Рекомендатель выбирает только доступные и исполнимые задачи.

**Шаги:**

1. Добавить topic weights, difficulty, estimated_minutes и поддерживаемый язык в публичные resource metadata.
2. Выдавать paginated internal snapshot опубликованных версий и tombstones.
3. Публиковать resource update при смене актуальной версии без hidden payload.
4. **Проверка после готовности функционала:** snapshot/replay, archived и смена версии.

<a id="tsk-11"></a>

## [tasks] Finish imported problem list integration

- **ID:** TSK-11
- **Оценка:** 2 SP (2 рабочих дня)
- **Этап:** F2 — пользовательские функции; **контур:** PROD — дополнение к полному продакшену.
- **Приоритет:** P2 — последующее улучшение.
- **Зависит от:** [HNT-01](../task-hunter/tasks.md#hnt-01).
- **Где менять:** `internal/usecase/problem_lists.go`, `internal/adapter/postgres/problem_lists.go`, `api/openapi.yaml`.
- **Готово, когда:** Повтор импорта не дублирует список; кандидаты не становятся публичными без модерации.

**Шаги:**

1. Сверить текущие незакоммиченные изменения, не создавать модель заново.
2. Закрепить порядок, уникальность source/list/task и поведение частичного импорта.
3. Выдавать обычному пользователю только опубликованные элементы; обновить OpenAPI.
4. **Проверка после готовности функционала:** component partial import, повтор и роли.

<a id="tsk-12"></a>

## [tasks] Add editorial publication checks

- **ID:** TSK-12
- **Оценка:** 2 SP (2 рабочих дня)
- **Этап:** F2 — пользовательские функции; **контур:** PROD — дополнение к полному продакшену.
- **Приоритет:** P2 — последующее улучшение.
- **Зависит от:** [TSK-03](#tsk-03), [TSK-10](#tsk-10).
- **Где менять:** `internal/usecase/tasks.go`, `internal/usecase/candidates.go`.
- **Готово, когда:** Импортированный текст без валидной проверки нельзя выдать за готовую graded задачу.

**Шаги:**

1. Проверять связь с canonical topic через entities API.
2. Требовать attribution, supported language, проверенный suite и bounded limits для programming.
3. Запускать эталонное решение только через sandbox как редакторскую проверку перед publish.
4. **Проверка после готовности функционала:** publish validation и неуспешный editorial run.

<a id="tsk-13"></a>

## [tasks] Add outcome replay and privacy cleanup

- **ID:** TSK-13
- **Оценка:** 2 SP (2 рабочих дня)
- **Этап:** F3 — выпуск и эксплуатация; **контур:** PROD — дополнение к полному продакшену.
- **Приоритет:** P2 — последующее улучшение.
- **Зависит от:** [TSK-09](#tsk-09), [USR-09](../users/tasks.md#usr-09), [DOC-05](../docs/tasks.md#doc-05).
- **Где менять:** `internal/adapter/postgres`, `internal/transport/httpapi`.
- **Готово, когда:** История восстанавливается без БД другого сервиса, удалённый пользователь не возвращается при replay.

**Шаги:**

1. Выдать защищённый paginated replay учебных результатов по watermark.
2. Удалять исходники и идентифицирующие поля по политике, сохраняя допустимые агрегаты.
3. Обрабатывать deletion event идемпотентно с tombstone против старых событий.
4. **Проверка после готовности функционала:** export/delete/replay и expiry на отдельной БД.

<a id="tsk-14"></a>

## [tasks] Add execution operational metrics

- **ID:** TSK-14
- **Оценка:** 1 SP (1 рабочий день)
- **Этап:** F3 — выпуск и эксплуатация; **контур:** PROD — дополнение к полному продакшену.
- **Приоритет:** P2 — последующее улучшение.
- **Зависит от:** [TSK-05](#tsk-05), [PKR-05](../parker/tasks.md#pkr-05).
- **Где менять:** `internal/transport/kafka`, `internal/app/container.go`.
- **Готово, когда:** Дежурный отличает нехватку runner capacity от ошибок пользовательского кода.

**Шаги:**

1. Добавить queue age, attempts, rejected result, final latency и verdict counters.
2. Пропагировать trace/correlation, не использовать user/task IDs как labels.
3. Описать safe replay и диагностику stuck jobs.
4. **Проверка после готовности функционала:** metric label и log-redaction проверки.

<a id="tsk-15"></a>

## [tasks] Retry failed result records before polling ahead

- **ID:** TSK-15
- **Оценка:** 1 SP (1 рабочий день)
- **Этап:** F1 — надёжная основа; **контур:** PILOT — обязательный срез.
- **Приоритет:** P0 — блокер корректности/безопасности.
- **Зависит от:** [TSK-01](#tsk-01).
- **Где менять:** `internal/transport/kafka/results.go`, `internal/transport/kafka/client.go`.
- **Готово, когда:** Недоступность БД не теряет ответ sandbox при последующем успешном сообщении.

**Шаги:**

1. При transient HandleRunResult/commit error повторять текущую запись, а не переходить к следующему PollRecords.
2. Согласовать pause/rebalance и bounded backoff, не подтверждать offset после необработанной записи.
3. После исчерпания времени освобождать partition безопасно без потери result.
4. **Проверка после готовности функционала:** regression на DB failure offset N и success N+1 с restart/rebalance.

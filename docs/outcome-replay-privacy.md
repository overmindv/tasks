# Replay и privacy Tasks (OVM-253)

`TASKS_OUTCOME_REPLAY_TOKEN` открывает GET `/v1/internal/outcomes/manifest` и
`/v1/internal/outcomes/replay` по отдельному `Authorization: Bearer` credential.
Токен 32–512 печатных ASCII символов; не совпадает с privacy/resource/ingest/metrics.
Без токена маршруты отсутствуют. Ответы `no-store`, timeout 10 секунд.

Сначала manifest без параметров: `watermark`, `privacy_epoch`, `count`, `sha256`,
`legacy_missing`, `complete_since`. Replay принимает watermark/epoch manifest,
`after=0`, `limit=1..100`; возвращает `events[{position,sha256,event}]`, `through`,
`more`. Продолжение — after=through. Hash события — SHA256 канонического JSON
(сортировка ключей, числа без float); manifest hash — SHA256 конкатенации бинарных
32-byte event hashes по position. Проверять count/hash и повторно manifest с той же
границей перед переключением проекции. Новый epoch даёт 409 и требует нового replay.
Clock фиксирует порядок commit, а не выдачи sequence; gaps допустимы.

Outbox publication очищает payload, journal остаётся. Migration восстанавливает
только сохранившиеся payload. `legacy_missing>0` означает неполную старую историю:
нельзя объявлять полный rebuild или переключать полную проекцию без отдельного
решения по gap. Старые результаты не реконструируются из текущих verdict.

`TASKS_PRIVACY_TOKEN` и `TASKS_USERS_URL` задаются вместе. HTTPS обязателен,
HTTP разрешён только loopback. Этот token — Tasks credential из Users
`USERS_PRIVACY_OWNERS`. Users сначала обновить до контракта OVM-253: Tasks kinds
`submission`, `attempt`, `task_revision`, `task_review`, text 256 KiB, page 2 MiB.
GET `/internal/privacy/export` использует `X-Privacy-Service-Token`, user_id,
cursor/watermark и limit до 100. Snapshot ограничен 10 MiB/10000 записей, пятью
snapshot на subject, живёт час. Переполнение завершает запрос ошибкой без обрезания.
Позднее редактирование не меняет страницы. Экспорт содержит только собственные
исходники, выбранные option IDs, редакции и metadata review; скрытые тесты,
правильные ответы и sandbox stdout/stderr исключены.

Worker каждые 10 секунд читает одну запись durable Users deletion ledger,
удаляет до 100 строк на таблицу за транзакцию, затем подтверждает Users. Cursor
сохраняется только после ack. Отказ/рестарт повторяет ту же операцию. Tombstone
предшествует удалению, предотвращает поздние записи и сохраняется бессрочно как
минимальная защита восстановления. Сначала уничтожаются приватные exports.
Идентификаторы автора/редактора/модератора обезличиваются; editorial proof общего
материала сохраняет verdict без actor/code/output. Публичный материал и attribution
внешнего источника/лицензии сохраняются: это не копия удаляемого профиля.

`TASKS_RAW_RETENTION` и `TASKS_OUTCOME_RETENTION` — Go duration, по умолчанию 0
(отключено), допустимо от 1h до 87600h. Значения выбираются отдельно после политики
OVM-128. Первый слой очищает код/вывод, второй — идентифицируемые attempts/outcomes.
Корректировки не продлевают исходный graded_at; retired aggregate не воскресает.
Любая очистка инвалидирует snapshots. Истёкшие exports чистятся по 10 snapshot.

Это локальная owner-реализация, не гарантия удаления из Kafka/backup/других сервисов.
Уже отправленные или находящиеся в полёте события блокируются tombstones потребителя.
Consumers обязаны применять собственный deletion ledger. Production rollout, выбор
сроков, очистка реальных данных и проверка полного стека не выполнялись.

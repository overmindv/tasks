-- +goose Up
-- Durable журнал публикаций задач для пересборки ленты Feed.
-- Не зависит от Kafka retention и очистки outbox: записи создаёт триггер
-- в транзакции смены статуса, а часы сохраняют порядок коммитов.
CREATE TABLE task_activity_clock (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    revision  bigint NOT NULL DEFAULT 0 CHECK (revision >= 0)
);
INSERT INTO task_activity_clock DEFAULT VALUES;

CREATE TABLE task_activity_changes (
    revision        bigint PRIMARY KEY CHECK (revision > 0),
    event_id        uuid NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    occurred_at     timestamptz NOT NULL DEFAULT statement_timestamp(),
    task_id         uuid NOT NULL REFERENCES tasks(id),
    task_version_id uuid,
    event_type      text NOT NULL CHECK (event_type IN ('task.published', 'task.unpublished')),
    title           text,
    topic_id        uuid,
    task_type       text,
    difficulty      text
);
CREATE INDEX task_activity_changes_task_idx ON task_activity_changes (task_id, revision DESC);

-- Исходное состояние: текущие опубликованные задачи попадают в журнал,
-- иначе пересборка ленты после очистки Kafka потеряет существующие карточки.
-- Точное время публикации неизвестно, поэтому occurred_at фиксируется миграцией.
WITH published AS (
    SELECT t.id AS task_id, t.current_version_id, v.title, v.topic_id, v.task_type, v.difficulty
    FROM tasks AS t
    JOIN task_versions AS v ON v.id = t.current_version_id
    WHERE t.status = 'published' AND t.deleted_at IS NULL
), numbered AS (
    SELECT published.*, row_number() OVER (ORDER BY published.task_id) AS revision FROM published
)
INSERT INTO task_activity_changes
    (revision, task_id, task_version_id, event_type, occurred_at, title, topic_id, task_type, difficulty)
SELECT revision, task_id, current_version_id, 'task.published', now(), title, topic_id, task_type, difficulty
FROM numbered;

UPDATE task_activity_clock
SET revision = COALESCE((SELECT max(revision) FROM task_activity_changes), 0);

-- +goose StatementBegin
CREATE FUNCTION record_task_activity_change() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
DECLARE
    next_revision bigint;
    was_published boolean := OLD.status = 'published' AND OLD.deleted_at IS NULL;
    is_published boolean := NEW.status = 'published' AND NEW.deleted_at IS NULL;
    version_rec task_versions%ROWTYPE;
BEGIN
    IF was_published = is_published THEN
        RETURN NEW;
    END IF;

    -- Блокировка часов сохраняет порядок коммитов, а не выдачи номеров:
    -- иначе читатель мог бы пропустить ещё не подтверждённую запись.
    UPDATE task_activity_clock SET revision = revision + 1 RETURNING revision INTO STRICT next_revision;

    IF is_published THEN
        SELECT * INTO version_rec FROM task_versions WHERE id = NEW.current_version_id;
        INSERT INTO task_activity_changes
            (revision, task_id, task_version_id, event_type, occurred_at, title, topic_id, task_type, difficulty)
        VALUES (next_revision, NEW.id, NEW.current_version_id, 'task.published', statement_timestamp(),
                version_rec.title, version_rec.topic_id, version_rec.task_type, version_rec.difficulty);
    ELSE
        INSERT INTO task_activity_changes (revision, task_id, task_version_id, event_type, occurred_at)
        VALUES (next_revision, NEW.id, OLD.current_version_id, 'task.unpublished', statement_timestamp());
    END IF;

    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER task_activity_change
AFTER UPDATE OF status, deleted_at ON tasks
FOR EACH ROW EXECUTE FUNCTION record_task_activity_change();

-- +goose Down
DROP TRIGGER task_activity_change ON tasks;
DROP FUNCTION record_task_activity_change();
DROP TABLE task_activity_changes, task_activity_clock;

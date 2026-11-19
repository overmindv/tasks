-- +goose Up
LOCK TABLE tasks IN SHARE ROW EXCLUSIVE MODE;

ALTER TABLE task_versions ADD COLUMN estimated_minutes integer
    CHECK (estimated_minutes BETWEEN 1 AND 1440);

CREATE TABLE task_resource_clock (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    revision bigint NOT NULL DEFAULT 0 CHECK (revision >= 0)
);
INSERT INTO task_resource_clock DEFAULT VALUES;

CREATE TABLE task_resource_index (
    task_id uuid PRIMARY KEY REFERENCES tasks(id),
    first_revision bigint NOT NULL CHECK (first_revision > 0)
);

CREATE TABLE task_resource_changes (
    revision bigint PRIMARY KEY CHECK (revision > 0),
    event_id uuid NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    occurred_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    task_id uuid NOT NULL REFERENCES tasks(id),
    task_version_id uuid,
    topic_id uuid,
    title text,
    task_type text,
    difficulty text,
    estimated_minutes integer CHECK (estimated_minutes BETWEEN 1 AND 1440),
    language_id text CHECK (language_id IN ('python', 'go')),
    language_version text,
    FOREIGN KEY (task_id, task_version_id) REFERENCES task_versions(task_id, id),
    CHECK ((language_id IS NULL) = (language_version IS NULL)),
    CHECK (
        (task_version_id IS NULL AND topic_id IS NULL AND title IS NULL AND task_type IS NULL
            AND difficulty IS NULL AND estimated_minutes IS NULL AND language_id IS NULL)
        OR (task_version_id IS NOT NULL AND topic_id IS NOT NULL AND title IS NOT NULL
            AND task_type IS NOT NULL AND difficulty IS NOT NULL
            AND ((task_type = 'programming') = (language_id IS NOT NULL)))
    )
);

CREATE INDEX task_resource_changes_history_idx ON task_resource_changes (task_id, revision DESC);

CREATE VIEW task_resource_eligible AS
SELECT t.id AS task_id, v.id AS task_version_id, v.topic_id, v.title, v.task_type,
       v.difficulty, v.estimated_minutes,
       s.definition->>'language' AS language_id,
       s.definition->>'language_version' AS language_version
FROM tasks AS t
JOIN task_versions AS v ON v.task_id = t.id AND v.id = t.current_version_id
LEFT JOIN task_version_suites AS s ON v.task_type = 'programming' AND s.task_version_id = v.id
JOIN LATERAL (
    SELECT count(*) AS options, count(*) FILTER (WHERE o.is_correct) AS correct
    FROM task_options AS o
    WHERE v.task_type IN ('single_choice', 'multiple_choice') AND o.task_version_id = v.id
) AS choice ON true
WHERE t.status = 'published' AND t.deleted_at IS NULL
    AND v.topic_id IS NOT NULL AND v.topic_id <> '00000000-0000-0000-0000-000000000000'::uuid
    AND (
        (v.task_type = 'programming'
            AND s.definition->>'language' IN ('python', 'go')
            AND s.definition->>'language_version' ~ '^[0-9]+\.[0-9]+(\.[0-9]+)?$'
            AND s.definition->>'checker' IN ('exact', 'trim_newlines'))
        OR (choice.options BETWEEN 2 AND 20
            AND ((v.task_type = 'single_choice' AND choice.correct = 1)
                OR (v.task_type = 'multiple_choice' AND choice.correct BETWEEN 1 AND choice.options - 1)))
    );

WITH initial AS (
    INSERT INTO task_resource_changes
        (revision, task_id, task_version_id, topic_id, title, task_type, difficulty,
         estimated_minutes, language_id, language_version)
    SELECT row_number() OVER (ORDER BY task_id), task_id, task_version_id, topic_id,
           title, task_type, difficulty, estimated_minutes, language_id, language_version
    FROM task_resource_eligible
    RETURNING task_id, revision
)
INSERT INTO task_resource_index (task_id, first_revision)
SELECT task_id, revision FROM initial;

UPDATE task_resource_clock
SET revision = COALESCE((SELECT max(revision) FROM task_resource_changes), 0);

-- +goose StatementBegin
CREATE FUNCTION record_task_resource_change() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
DECLARE
    resource task_resource_eligible%ROWTYPE;
    next_revision bigint;
BEGIN
    SELECT * INTO resource FROM task_resource_eligible WHERE task_id = NEW.id;
    IF resource.task_version_id IS NULL AND NOT EXISTS (
        SELECT 1 FROM task_resource_changes
        WHERE revision = (SELECT max(revision) FROM task_resource_changes WHERE task_id = NEW.id)
            AND task_version_id IS NOT NULL
    ) THEN
        RETURN NEW;
    END IF;

    -- Блокировка часов сохраняет порядок коммитов, а не выдачи номеров.
    UPDATE task_resource_clock SET revision = revision + 1 RETURNING revision INTO STRICT next_revision;
    INSERT INTO task_resource_index (task_id, first_revision)
    VALUES (NEW.id, next_revision) ON CONFLICT (task_id) DO NOTHING;
    INSERT INTO task_resource_changes
        (revision, task_id, task_version_id, topic_id, title, task_type, difficulty,
         estimated_minutes, language_id, language_version)
    VALUES (next_revision, NEW.id, resource.task_version_id, resource.topic_id, resource.title,
            resource.task_type, resource.difficulty, resource.estimated_minutes,
            resource.language_id, resource.language_version);
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER task_resource_change
AFTER UPDATE OF current_version_id, status, deleted_at ON tasks
FOR EACH ROW
WHEN ((OLD.status = 'published' OR NEW.status = 'published')
    AND (OLD.status, OLD.current_version_id, OLD.deleted_at)
        IS DISTINCT FROM (NEW.status, NEW.current_version_id, NEW.deleted_at))
EXECUTE FUNCTION record_task_resource_change();

-- +goose Down
DROP TRIGGER task_resource_change ON tasks;
DROP FUNCTION record_task_resource_change();
DROP VIEW task_resource_eligible;
DROP TABLE task_resource_changes, task_resource_index, task_resource_clock;
ALTER TABLE task_versions DROP COLUMN estimated_minutes;

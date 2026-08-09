-- +goose Up
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE tasks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    current_version_id UUID,
    status TEXT NOT NULL DEFAULT 'draft',
    created_by UUID NOT NULL,
    updated_by UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ,
    CONSTRAINT tasks_status_check CHECK (status IN ('draft', 'published', 'archived'))
);

CREATE TABLE task_versions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_id UUID NOT NULL REFERENCES tasks(id),
    version_number INTEGER NOT NULL,
    topic_id UUID,
    title TEXT NOT NULL,
    statement TEXT NOT NULL,
    task_type TEXT NOT NULL,
    difficulty TEXT NOT NULL,
    created_by UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT task_versions_version_check CHECK (version_number > 0),
    CONSTRAINT task_versions_title_check CHECK (char_length(title) BETWEEN 1 AND 200),
    CONSTRAINT task_versions_statement_check CHECK (char_length(statement) BETWEEN 1 AND 50000),
    CONSTRAINT task_versions_type_check CHECK (task_type IN ('single_choice', 'multiple_choice')),
    CONSTRAINT task_versions_difficulty_check CHECK (difficulty IN ('easy', 'medium', 'hard')),
    CONSTRAINT task_versions_task_number_key UNIQUE (task_id, version_number),
    CONSTRAINT task_versions_task_id_key UNIQUE (task_id, id)
);

CREATE TABLE task_options (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_version_id UUID NOT NULL REFERENCES task_versions(id),
    text TEXT NOT NULL,
    is_correct BOOLEAN NOT NULL,
    position INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT task_options_text_check CHECK (char_length(text) BETWEEN 1 AND 2000),
    CONSTRAINT task_options_position_check CHECK (position >= 0),
    CONSTRAINT task_options_version_position_key UNIQUE (task_version_id, position),
    CONSTRAINT task_options_version_id_key UNIQUE (task_version_id, id)
);

ALTER TABLE tasks
    ADD CONSTRAINT tasks_current_version_fk FOREIGN KEY (id, current_version_id)
        REFERENCES task_versions(task_id, id);

CREATE TABLE submissions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL,
    task_id UUID NOT NULL REFERENCES tasks(id),
    task_version_id UUID NOT NULL,
    idempotency_key UUID NOT NULL,
    request_hash TEXT NOT NULL,
    verdict TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT submissions_verdict_check CHECK (verdict IN ('accepted', 'wrong_answer')),
    CONSTRAINT submissions_task_version_fk FOREIGN KEY (task_id, task_version_id)
        REFERENCES task_versions(task_id, id),
    CONSTRAINT submissions_user_idempotency_key UNIQUE (user_id, idempotency_key),
    CONSTRAINT submissions_id_version_key UNIQUE (id, task_version_id)
);

CREATE TABLE submission_answers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    submission_id UUID NOT NULL,
    task_version_id UUID NOT NULL,
    option_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT submission_answers_submission_version_fk FOREIGN KEY (submission_id, task_version_id)
        REFERENCES submissions(id, task_version_id),
    CONSTRAINT submission_answers_option_version_fk FOREIGN KEY (task_version_id, option_id)
        REFERENCES task_options(task_version_id, id),
    CONSTRAINT submission_answers_submission_option_key UNIQUE (submission_id, option_id)
);

CREATE INDEX tasks_status_created_idx ON tasks (status, created_at DESC, id DESC) WHERE deleted_at IS NULL;
CREATE INDEX task_versions_current_idx ON task_versions (task_id, version_number DESC);
CREATE INDEX task_options_version_position_idx ON task_options (task_version_id, position);
CREATE INDEX submissions_user_created_idx ON submissions (user_id, created_at DESC, id DESC);
CREATE INDEX submissions_user_task_created_idx ON submissions (user_id, task_id, created_at DESC, id DESC);
CREATE INDEX submission_answers_submission_idx ON submission_answers (submission_id);

-- +goose Down
ALTER TABLE tasks DROP CONSTRAINT IF EXISTS tasks_current_version_fk;
DROP TABLE IF EXISTS submission_answers;
DROP TABLE IF EXISTS submissions;
DROP TABLE IF EXISTS task_options;
DROP TABLE IF EXISTS task_versions;
DROP TABLE IF EXISTS tasks;

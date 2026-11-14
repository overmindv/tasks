-- +goose Up
-- Сущность «список задач»: именованный набор задач из внешнего problem-list
-- (например, leetcode /problem-list/{slug}). Создаётся идемпотентно по source_url
-- при импорте кандидатов из task-hunter.
CREATE TABLE problem_lists (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    source_id TEXT NOT NULL,
    source_url TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT problem_lists_name_check CHECK (char_length(name) BETWEEN 1 AND 200),
    CONSTRAINT problem_lists_source_id_check CHECK (char_length(source_id) BETWEEN 1 AND 100),
    CONSTRAINT problem_lists_source_url_check CHECK (char_length(source_url) BETWEEN 1 AND 1000),
    CONSTRAINT problem_lists_source_url_unique UNIQUE (source_url)
);

-- Члены списка — опубликованные задачи в порядке сбора.
CREATE TABLE problem_list_members (
    problem_list_id UUID NOT NULL REFERENCES problem_lists(id) ON DELETE CASCADE,
    task_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    position INTEGER NOT NULL,
    CONSTRAINT problem_list_members_pkey PRIMARY KEY (problem_list_id, task_id),
    CONSTRAINT problem_list_members_position_check CHECK (position >= 0)
);

-- Связь кандидата (модерация) со списком: при одобрении кандидатской задачи
-- создаётся членство опубликованной задачи в списке.
ALTER TABLE task_candidates
    ADD COLUMN problem_list_id UUID REFERENCES problem_lists(id);

CREATE INDEX task_candidates_problem_list_idx
    ON task_candidates (problem_list_id, created_at DESC, id DESC);

-- +goose Down
DROP INDEX IF EXISTS task_candidates_problem_list_idx;
ALTER TABLE task_candidates DROP COLUMN IF EXISTS problem_list_id;
DROP TABLE IF EXISTS problem_list_members;
DROP TABLE IF EXISTS problem_lists;

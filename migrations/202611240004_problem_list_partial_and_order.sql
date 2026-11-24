-- +goose Up
-- Признак неполного problem-list: источник (task-hunter) помечает список,
-- если внешний API отдал не весь набор или часть деталей не загрузилась.
ALTER TABLE problem_lists ADD COLUMN IF NOT EXISTS truncated BOOLEAN NOT NULL DEFAULT false;

-- Позиция кандидата в порядке сбора; при одобрении она переносится
-- в problem_list_members, поэтому порядок списка не зависит от порядка модерации.
ALTER TABLE task_candidates ADD COLUMN IF NOT EXISTS problem_list_position INTEGER;
ALTER TABLE task_candidates
    ADD CONSTRAINT task_candidates_problem_list_position_check
    CHECK (problem_list_position IS NULL OR problem_list_position >= 0);

-- +goose Down
ALTER TABLE task_candidates DROP CONSTRAINT IF EXISTS task_candidates_problem_list_position_check;
ALTER TABLE task_candidates DROP COLUMN IF EXISTS problem_list_position;
ALTER TABLE problem_lists DROP COLUMN IF EXISTS truncated;

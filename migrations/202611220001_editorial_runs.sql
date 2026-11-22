-- +goose Up
ALTER TABLE code_submissions DROP CONSTRAINT code_submissions_run_mode_check;
ALTER TABLE code_submissions ADD CONSTRAINT code_submissions_run_mode_check
    CHECK (run_mode IN ('samples', 'graded', 'editorial'));
CREATE INDEX code_submissions_editorial_version ON code_submissions(task_id, task_version_id, created_at DESC, id DESC)
    WHERE run_mode = 'editorial';

-- +goose Down
-- Откат не удаляет редакционную историю; при её наличии ограничение не применится.
ALTER TABLE code_submissions DROP CONSTRAINT code_submissions_run_mode_check;
ALTER TABLE code_submissions ADD CONSTRAINT code_submissions_run_mode_check CHECK (run_mode IN ('samples', 'graded'));
DROP INDEX code_submissions_editorial_version;

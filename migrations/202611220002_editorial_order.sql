-- +goose Up
CREATE SEQUENCE editorial_run_order;
ALTER TABLE code_submissions ADD COLUMN editorial_sequence bigint;
CREATE UNIQUE INDEX code_submissions_editorial_sequence ON code_submissions(editorial_sequence)
    WHERE editorial_sequence IS NOT NULL;
DROP INDEX code_submissions_editorial_version;
CREATE INDEX code_submissions_editorial_version ON code_submissions(task_id, task_version_id, editorial_sequence DESC)
    WHERE run_mode = 'editorial' AND editorial_sequence IS NOT NULL;
-- Старые editorial без sequence не дают разрешения публикации: требуется новый run.

-- +goose Down
DROP INDEX code_submissions_editorial_version;
CREATE INDEX code_submissions_editorial_version ON code_submissions(task_id, task_version_id, created_at DESC, id DESC)
    WHERE run_mode = 'editorial';
DROP INDEX code_submissions_editorial_sequence;
ALTER TABLE code_submissions DROP COLUMN editorial_sequence;
DROP SEQUENCE editorial_run_order;

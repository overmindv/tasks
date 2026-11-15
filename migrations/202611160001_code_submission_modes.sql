-- +goose Up
ALTER TABLE code_submissions ADD COLUMN run_mode text NOT NULL DEFAULT 'samples'
    CHECK (run_mode IN ('samples', 'graded'));
-- +goose Down
ALTER TABLE code_submissions DROP COLUMN run_mode;

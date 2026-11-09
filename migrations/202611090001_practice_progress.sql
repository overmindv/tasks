-- +goose Up
CREATE TABLE practice_progress_visibility (
    user_id UUID PRIMARY KEY,
    is_public BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX submissions_accepted_rating_idx
    ON submissions (user_id, task_id, created_at, id)
    WHERE verdict = 'accepted';
CREATE INDEX code_submissions_accepted_rating_idx
    ON code_submissions (user_id, task_id, completed_at, id)
    WHERE status = 'completed' AND verdict = 'accepted';
CREATE INDEX practice_progress_visibility_public_idx
    ON practice_progress_visibility (user_id) WHERE is_public;

-- +goose Down
DROP INDEX IF EXISTS practice_progress_visibility_public_idx;
DROP INDEX IF EXISTS code_submissions_accepted_rating_idx;
DROP INDEX IF EXISTS submissions_accepted_rating_idx;
DROP TABLE IF EXISTS practice_progress_visibility;

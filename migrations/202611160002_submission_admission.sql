-- +goose Up
CREATE INDEX idx_code_submissions_pending_user ON code_submissions(user_id) WHERE status = 'queued';
-- +goose Down
DROP INDEX idx_code_submissions_pending_user;

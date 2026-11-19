-- +goose NO TRANSACTION
-- +goose Up
CREATE INDEX CONCURRENTLY code_submission_outbox_pending_created_idx
    ON code_submission_outbox (created_at)
    WHERE published_at IS NULL;

-- +goose Down
DROP INDEX CONCURRENTLY IF EXISTS code_submission_outbox_pending_created_idx;

-- +goose Up
ALTER TABLE code_execution_result_inbox
    DROP CONSTRAINT code_execution_result_inbox_status_check;
ALTER TABLE code_execution_result_inbox
    ADD CONSTRAINT code_execution_result_inbox_status_check
    CHECK (status IN ('processed', 'rejected', 'stale'));

-- +goose Down
-- Откат сохраняет технические записи поздних результатов.
UPDATE code_execution_result_inbox SET status = 'rejected', error_code = 'STALE_ATTEMPT'
WHERE status = 'stale';
ALTER TABLE code_execution_result_inbox
    DROP CONSTRAINT code_execution_result_inbox_status_check;
ALTER TABLE code_execution_result_inbox
    ADD CONSTRAINT code_execution_result_inbox_status_check
    CHECK (status IN ('processed', 'rejected'));

-- +goose Up
-- Retry-механика программных решений (блок Б аудита):
-- sandbox может ответить infrastructure_error/checker_error или не ответить вовсе.
-- attempts лимитирует число переотправок, next_attempt_at управляет reaper-ом.

ALTER TABLE code_submissions
    ADD COLUMN attempts        INTEGER NOT NULL DEFAULT 1,
    ADD COLUMN next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now();

ALTER TABLE code_submissions
    ADD CONSTRAINT code_submissions_attempts_check CHECK (attempts >= 1);

-- Reaper ищет заброшенные queued-запуски по расписанию следующей попытки.
CREATE INDEX code_submissions_next_attempt_idx
    ON code_submissions (next_attempt_at)
    WHERE status = 'queued';

-- +goose Down
DROP INDEX IF EXISTS code_submissions_next_attempt_idx;

ALTER TABLE code_submissions
    DROP CONSTRAINT code_submissions_attempts_check;

ALTER TABLE code_submissions
    DROP COLUMN attempts,
    DROP COLUMN next_attempt_at;

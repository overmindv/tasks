-- +goose Up
-- outbox — стандартная parker-таблица исходящих событий (parker.OutboxSchema):
-- используется ВТОРЫМ диспетчером tasks для публикации business-события
-- "задача опубликована" в топик активности, в отличие от code_submission_outbox
-- для выполнения кода (таблица сохраняется отдельно из-за FK на code_submissions).
CREATE TABLE IF NOT EXISTS outbox (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    topic           TEXT NOT NULL,
    message_key     TEXT NOT NULL,
    message_value   BYTEA NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',
    attempts        INT  NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at         TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS outbox_status_idx ON outbox(status, next_attempt_at);

-- +goose Down
DROP TABLE IF EXISTS outbox;

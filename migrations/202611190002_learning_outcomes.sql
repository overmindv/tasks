-- +goose Up
CREATE TABLE learning_outcome_outbox (
    id uuid PRIMARY KEY,
    aggregate_id uuid NOT NULL,
    correction_version integer NOT NULL CHECK (correction_version >= 1),
    topic text NOT NULL CHECK (topic = 'learning.outcome.v1'),
    message_key text NOT NULL CHECK (message_key = aggregate_id::text),
    payload jsonb,
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    available_at timestamptz NOT NULL DEFAULT now(),
    claimed_until timestamptz,
    claim_token uuid,
    published_at timestamptz,
    last_error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (aggregate_id, correction_version),
    CHECK ((published_at IS NULL) = (payload IS NOT NULL)),
    CHECK (payload IS NULL OR (jsonb_typeof(payload) = 'object' AND octet_length(payload::text) <= 2048))
);

CREATE INDEX learning_outcome_due_idx ON learning_outcome_outbox (available_at, created_at, id)
    WHERE published_at IS NULL;

-- +goose Down
DROP TABLE learning_outcome_outbox;

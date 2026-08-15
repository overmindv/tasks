-- +goose Up
CREATE TABLE code_submissions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL,
    task_id UUID NOT NULL REFERENCES tasks(id),
    task_version_id UUID NOT NULL,
    execution_id UUID NOT NULL,
    correlation_id UUID NOT NULL,
    idempotency_key UUID NOT NULL,
    request_hash TEXT NOT NULL,
    language TEXT NOT NULL,
    source_file_name TEXT NOT NULL,
    source_code TEXT,
    status TEXT NOT NULL DEFAULT 'queued',
    verdict TEXT,
    compilation_result JSONB,
    execution_result JSONB,
    test_results JSONB NOT NULL DEFAULT '[]'::jsonb,
    error_code TEXT,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    CONSTRAINT code_submissions_task_version_fk FOREIGN KEY (task_id, task_version_id)
        REFERENCES task_versions(task_id, id),
    CONSTRAINT code_submissions_execution_key UNIQUE (execution_id),
    CONSTRAINT code_submissions_user_idempotency_key UNIQUE (user_id, idempotency_key),
    CONSTRAINT code_submissions_language_check CHECK (language IN ('python', 'go')),
    CONSTRAINT code_submissions_source_name_check CHECK (char_length(source_file_name) BETWEEN 1 AND 255),
    CONSTRAINT code_submissions_source_size_check CHECK (
        source_code IS NULL OR octet_length(source_code) BETWEEN 1 AND 262144
    ),
    CONSTRAINT code_submissions_status_check CHECK (status IN ('queued', 'completed')),
    CONSTRAINT code_submissions_verdict_check CHECK (
        verdict IS NULL OR verdict IN (
            'accepted',
            'wrong_answer',
            'compilation_error',
            'runtime_error',
            'time_limit_exceeded',
            'memory_limit_exceeded',
            'output_limit_exceeded',
            'checker_error',
            'infrastructure_error',
            'cancelled'
        )
    ),
    CONSTRAINT code_submissions_completion_check CHECK (
        (status = 'queued' AND verdict IS NULL AND completed_at IS NULL) OR
        (status = 'completed' AND verdict IS NOT NULL AND completed_at IS NOT NULL)
    )
);

CREATE TABLE code_submission_outbox (
    id UUID PRIMARY KEY,
    aggregate_id UUID NOT NULL REFERENCES code_submissions(id) ON DELETE CASCADE,
    topic TEXT NOT NULL,
    message_key TEXT NOT NULL,
    payload JSONB,
    attempts INTEGER NOT NULL DEFAULT 0,
    available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_until TIMESTAMPTZ,
    claim_token UUID,
    published_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT code_submission_outbox_attempts_check CHECK (attempts >= 0),
    CONSTRAINT code_submission_outbox_payload_check CHECK (
        (published_at IS NULL AND payload IS NOT NULL) OR
        (published_at IS NOT NULL AND payload IS NULL)
    )
);

CREATE TABLE code_execution_result_inbox (
    id UUID PRIMARY KEY,
    event_id UUID,
    topic TEXT NOT NULL,
    partition INTEGER NOT NULL,
    message_offset BIGINT NOT NULL,
    payload_sha256 TEXT NOT NULL,
    status TEXT NOT NULL,
    error_code TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT code_execution_result_inbox_status_check CHECK (status IN ('processed', 'rejected')),
    CONSTRAINT code_execution_result_inbox_hash_check CHECK (char_length(payload_sha256) = 64),
    CONSTRAINT code_execution_result_inbox_message_key UNIQUE (topic, partition, message_offset)
);

CREATE INDEX code_submissions_user_created_idx
    ON code_submissions (user_id, created_at DESC, id DESC);
CREATE INDEX code_submissions_user_task_created_idx
    ON code_submissions (user_id, task_id, created_at DESC, id DESC);
CREATE INDEX code_submissions_status_created_idx
    ON code_submissions (status, created_at, id);
CREATE INDEX code_submission_outbox_due_idx
    ON code_submission_outbox (available_at, created_at, id)
    WHERE published_at IS NULL;
CREATE UNIQUE INDEX code_execution_result_inbox_event_key
    ON code_execution_result_inbox (event_id)
    WHERE event_id IS NOT NULL;

-- +goose Down
DROP TABLE IF EXISTS code_execution_result_inbox;
DROP TABLE IF EXISTS code_submission_outbox;
DROP TABLE IF EXISTS code_submissions;

-- +goose Up
CREATE TABLE code_submission_replay_audit (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
 submission_id uuid NOT NULL,
 previous_execution_id uuid NOT NULL,
 next_execution_id uuid NOT NULL,
 actor_id uuid NOT NULL,
 previous_attempts integer NOT NULL,
 created_at timestamptz NOT NULL,
 UNIQUE(submission_id,previous_execution_id)
);
-- +goose Down
DROP TABLE code_submission_replay_audit;

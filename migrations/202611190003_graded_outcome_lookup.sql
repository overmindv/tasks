-- +goose NO TRANSACTION
-- +goose Up
CREATE INDEX CONCURRENTLY code_submissions_graded_outcome_idx
    ON code_submissions (user_id, task_id, created_at DESC, id DESC)
    WHERE status = 'completed' AND run_mode = 'graded'
      AND verdict IN ('accepted', 'wrong_answer', 'compilation_error', 'runtime_error',
                      'time_limit_exceeded', 'memory_limit_exceeded', 'output_limit_exceeded');

-- +goose Down
DROP INDEX CONCURRENTLY code_submissions_graded_outcome_idx;

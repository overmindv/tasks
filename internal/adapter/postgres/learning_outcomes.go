package postgresadapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/overmindv/tasks/internal/domain"
)

func (r *Postgres) FirstLearningOutcome(ctx context.Context, learner, task, submission uuid.UUID) (bool, error) {
	if _, ok := r.query.(pgx.Tx); !ok {
		return false, errors.New("first learning outcome requires a transaction")
	}
	// Результаты разных запусков могут прийти одновременно и в любом порядке.
	if _, err := r.query.Exec(ctx, `
		SELECT pg_advisory_xact_lock(hashtextextended('learning-outcome:' || $1::uuid::text || ':' || $2::uuid::text, 0))`,
		learner, task); err != nil {
		return false, fmt.Errorf("lock learner outcome: %w", err)
	}
	var first bool
	err := r.query.QueryRow(ctx, `
		SELECT (SELECT created_by FROM tasks WHERE id = $2) <> $1
		   AND (SELECT 1 FROM task_versions WHERE task_id = $2 AND created_by = $1 LIMIT 1) IS NULL
		   AND (SELECT 1 FROM submissions WHERE user_id = $1 AND task_id = $2 AND id <> $3
		        ORDER BY created_at DESC, id DESC LIMIT 1) IS NULL
		   AND (SELECT 1 FROM code_submissions
		        WHERE user_id = $1 AND task_id = $2 AND id <> $3 AND status = 'completed' AND run_mode = 'graded'
		          AND verdict IN ('accepted', 'wrong_answer', 'compilation_error', 'runtime_error',
		                          'time_limit_exceeded', 'memory_limit_exceeded', 'output_limit_exceeded')
		        ORDER BY created_at DESC, id DESC LIMIT 1) IS NULL`, learner, task, submission).Scan(&first)
	if err != nil {
		return false, fmt.Errorf("look up graded task history: %w", err)
	}
	return first, nil
}

func (r *Postgres) InsertLearningOutcome(ctx context.Context, event domain.LearningOutcome) error {
	if _, ok := r.query.(pgx.Tx); !ok {
		return errors.New("learning outcome requires a transaction")
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode learning outcome: %w", err)
	}
	_, err = r.query.Exec(ctx, `
		INSERT INTO learning_outcome_outbox (id, aggregate_id, correction_version, topic, message_key, payload)
		VALUES ($1, $2, $3, $4, $5, $6::jsonb)`,
		event.EventID, event.AggregateID, event.Payload.CorrectionVersion, domain.LearningOutcomeTopic, event.AggregateID.String(), payload)
	if err != nil {
		return fmt.Errorf("insert learning outcome: %w", err)
	}
	return nil
}

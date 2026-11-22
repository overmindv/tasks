package postgresadapter

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/overmindv/tasks/internal/domain"
)

// ApplyAccountDeletion обрабатывает ограниченный срез одного запроса под actor lock.
func (r *Postgres) ApplyAccountDeletion(ctx context.Context, event domain.AccountDeletion, batch int) (bool, error) {
	if event.UserID == uuid.Nil || event.EventID == uuid.Nil || event.Version < 1 || batch < 1 || batch > 100 {
		return false, domain.ErrOutcomeSnapshot
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('tasks-user:'||$1::uuid::text,0))`, event.UserID); err != nil {
		return false, err
	}
	tag, err := tx.Exec(ctx, `INSERT INTO task_privacy_tombstones(user_id,event_id,version) VALUES($1,$2,$3) ON CONFLICT DO NOTHING`, event.UserID, event.EventID, event.Version)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() > 0 {
		if _, err = tx.Exec(ctx, `UPDATE outcome_journal_clock SET privacy_epoch=privacy_epoch+1 WHERE singleton`); err != nil {
			return false, err
		}
	}
	if _, err = tx.Exec(ctx, `DELETE FROM practice_progress_visibility WHERE user_id=$1`, event.UserID); err != nil {
		return false, err
	}
	if _, err = tx.Exec(ctx, `DELETE FROM task_export_snapshots WHERE id IN(SELECT id FROM task_export_snapshots WHERE user_id=$1 ORDER BY id LIMIT $2)`, event.UserID, 10); err != nil {
		return false, err
	}
	if err = eraseLearnerRows(ctx, tx, event.UserID, batch); err != nil {
		return false, err
	}
	for _, query := range []string{
		`UPDATE tasks SET updated_by='00000000-0000-0000-0000-000000000000' WHERE id IN(SELECT id FROM tasks WHERE updated_by=$1 ORDER BY id LIMIT $2)`,
		`UPDATE tasks SET created_by='00000000-0000-0000-0000-000000000000' WHERE id IN(SELECT id FROM tasks WHERE created_by=$1 ORDER BY id LIMIT $2)`,
		`UPDATE task_versions SET created_by='00000000-0000-0000-0000-000000000000' WHERE id IN(SELECT id FROM task_versions WHERE created_by=$1 ORDER BY id LIMIT $2)`,
		`UPDATE task_candidates SET reviewed_by=NULL WHERE id IN(SELECT id FROM task_candidates WHERE reviewed_by=$1 ORDER BY id LIMIT $2)`,
		`UPDATE code_submission_replay_audit SET actor_id='00000000-0000-0000-0000-000000000000' WHERE id IN(SELECT id FROM code_submission_replay_audit WHERE actor_id=$1 ORDER BY id LIMIT $2)`,
		`DELETE FROM learning_outcome_outbox WHERE id IN(SELECT id FROM learning_outcome_outbox WHERE payload->'payload'->>'learner_id'=$1::uuid::text ORDER BY id LIMIT $2)`,
	} {
		if _, err = tx.Exec(ctx, query, event.UserID, batch); err != nil {
			return false, err
		}
	}
	var remaining bool
	err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM submissions WHERE user_id=$1) OR EXISTS(SELECT 1 FROM code_submissions WHERE user_id=$1)
 OR EXISTS(SELECT 1 FROM outcome_journal WHERE learner_id=$1) OR EXISTS(SELECT 1 FROM tasks WHERE created_by=$1)
 OR EXISTS(SELECT 1 FROM tasks WHERE updated_by=$1) OR EXISTS(SELECT 1 FROM task_versions WHERE created_by=$1) OR EXISTS(SELECT 1 FROM task_candidates WHERE reviewed_by=$1)
 OR EXISTS(SELECT 1 FROM code_submission_replay_audit WHERE actor_id=$1)
 OR EXISTS(SELECT 1 FROM learning_outcome_outbox WHERE payload->'payload'->>'learner_id'=$1::uuid::text)
 OR EXISTS(SELECT 1 FROM task_export_snapshots WHERE user_id=$1)`, event.UserID).Scan(&remaining)
	if err != nil {
		return false, err
	}
	if !remaining {
		if _, err = tx.Exec(ctx, `UPDATE task_privacy_tombstones SET completed=true WHERE user_id=$1`, event.UserID); err != nil {
			return false, err
		}
	}

	return !remaining, tx.Commit(ctx)
}

func eraseLearnerRows(ctx context.Context, tx pgx.Tx, userID uuid.UUID, batch int) error {
	var codeIDs, choiceIDs []uuid.UUID
	err := tx.QueryRow(ctx, `SELECT COALESCE(array_agg(id),'{}'::uuid[]) FROM(SELECT id FROM code_submissions WHERE user_id=$1 ORDER BY id LIMIT $2 FOR UPDATE) p`, userID, batch).Scan(&codeIDs)
	if err != nil {
		return err
	}
	err = tx.QueryRow(ctx, `SELECT COALESCE(array_agg(id),'{}'::uuid[]) FROM(SELECT id FROM submissions WHERE user_id=$1 ORDER BY id LIMIT $2 FOR UPDATE) p`, userID, batch).Scan(&choiceIDs)
	if err != nil {
		return err
	}
	all := append(append([]uuid.UUID{}, codeIDs...), choiceIDs...)
	if _, err = tx.Exec(ctx, `INSERT INTO outcome_retired_aggregates(aggregate_id) SELECT unnest($1::uuid[]) ON CONFLICT DO NOTHING`, all); err != nil {
		return err
	}
	for _, query := range []string{
		`DELETE FROM code_submission_outbox WHERE aggregate_id=ANY($1::uuid[])`,
		// Сохраняем независимую editorial-проверку общего материала без actor/source/output.
		`UPDATE code_submissions SET user_id='00000000-0000-0000-0000-000000000000',source_code=NULL,source_file_name='removed',
 request_hash=repeat('0',64),idempotency_key=gen_random_uuid(),compilation_result=NULL,execution_result=NULL,test_results='[]',error_code=NULL,error_message=NULL,
 verdict=CASE WHEN status='queued' THEN 'cancelled' ELSE verdict END,status='completed',completed_at=COALESCE(completed_at,clock_timestamp())
 WHERE id=ANY($1::uuid[]) AND run_mode='editorial'`,
		`DELETE FROM code_submissions WHERE id=ANY($1::uuid[]) AND run_mode<>'editorial'`,
	} {
		if _, err = tx.Exec(ctx, query, codeIDs); err != nil {
			return err
		}
	}
	if _, err = tx.Exec(ctx, `DELETE FROM submission_answers WHERE submission_id=ANY($1::uuid[])`, choiceIDs); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `DELETE FROM submissions WHERE id=ANY($1::uuid[])`, choiceIDs); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `WITH removed AS(DELETE FROM outcome_journal WHERE position IN(SELECT position FROM outcome_journal WHERE learner_id=$1 ORDER BY position LIMIT $2) RETURNING aggregate_id)
 INSERT INTO outcome_retired_aggregates(aggregate_id) SELECT DISTINCT aggregate_id FROM removed ON CONFLICT DO NOTHING`, userID, batch)

	return err
}

func (r *Postgres) PrivacyPosition(ctx context.Context) (int64, error) {
	var position int64
	err := r.query.QueryRow(ctx, `SELECT position FROM task_privacy_sync WHERE singleton`).Scan(&position)

	return position, err
}
func (r *Postgres) AdvancePrivacyPosition(ctx context.Context, position int64) error {
	_, err := r.query.Exec(ctx, `UPDATE task_privacy_sync SET position=GREATEST(position,$1) WHERE singleton`, position)

	return err
}

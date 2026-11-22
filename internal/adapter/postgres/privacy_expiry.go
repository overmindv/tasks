package postgresadapter

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/domain"
)

// ExpireLearningData получает уже выбранные usecase пороги; нулевое время отключает слой.
func (r *Postgres) ExpireLearningData(ctx context.Context, rawBefore, historyBefore time.Time, batch int) error {
	if batch < 1 || batch > 100 {
		return domain.ErrOutcomeSnapshot
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// Общая граница с INSERT outcome: retired проверяется после получения clock lock.
	if _, err = tx.Exec(ctx, `SELECT position FROM outcome_journal_clock WHERE singleton FOR UPDATE`); err != nil {
		return err
	}
	changed := false
	if !rawBefore.IsZero() {
		var ids []uuid.UUID
		err = tx.QueryRow(ctx, `SELECT COALESCE(array_agg(id),'{}'::uuid[]) FROM(SELECT id FROM code_submissions
  WHERE COALESCE(completed_at,created_at)<$1 AND (source_code IS NOT NULL OR compilation_result IS NOT NULL OR execution_result IS NOT NULL OR test_results<>'[]'::jsonb OR error_message IS NOT NULL)
  ORDER BY COALESCE(completed_at,created_at),id LIMIT $2 FOR UPDATE SKIP LOCKED)p`, rawBefore, batch).Scan(&ids)
		if err != nil {
			return err
		}
		if len(ids) > 0 {
			changed = true
			if _, err = tx.Exec(ctx, `DELETE FROM code_submission_outbox WHERE aggregate_id=ANY($1::uuid[])`, ids); err != nil {
				return err
			}
			_, err = tx.Exec(ctx, `UPDATE code_submissions SET source_code=NULL,compilation_result=NULL,execution_result=NULL,test_results='[]',error_message=NULL,
    verdict=CASE WHEN status='queued' THEN 'cancelled' ELSE verdict END,status='completed',completed_at=COALESCE(completed_at,clock_timestamp()) WHERE id=ANY($1::uuid[])`, ids)
			if err != nil {
				return err
			}
		}
	}
	if !historyBefore.IsZero() {
		var journalIDs, codeIDs, choiceIDs []uuid.UUID
		err = tx.QueryRow(ctx, `SELECT COALESCE(array_agg(event_id),'{}'::uuid[]) FROM(SELECT event_id FROM outcome_journal WHERE original_graded_at<$1 ORDER BY position LIMIT $2 FOR UPDATE SKIP LOCKED)p`, historyBefore, batch).Scan(&journalIDs)
		if err != nil {
			return err
		}
		err = tx.QueryRow(ctx, `SELECT COALESCE(array_agg(id),'{}'::uuid[]) FROM(SELECT id FROM code_submissions WHERE run_mode<>'editorial' AND COALESCE(completed_at,created_at)<$1 ORDER BY id LIMIT $2 FOR UPDATE SKIP LOCKED)p`, historyBefore, batch).Scan(&codeIDs)
		if err != nil {
			return err
		}
		err = tx.QueryRow(ctx, `SELECT COALESCE(array_agg(id),'{}'::uuid[]) FROM(SELECT id FROM submissions WHERE created_at<$1 ORDER BY id LIMIT $2 FOR UPDATE SKIP LOCKED)p`, historyBefore, batch).Scan(&choiceIDs)
		if err != nil {
			return err
		}
		if len(journalIDs)+len(codeIDs)+len(choiceIDs) > 0 {
			changed = true
		}
		all := append(append([]uuid.UUID{}, codeIDs...), choiceIDs...)
		if _, err = tx.Exec(ctx, `INSERT INTO outcome_retired_aggregates(aggregate_id) SELECT unnest($1::uuid[]) ON CONFLICT DO NOTHING`, all); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `INSERT INTO outcome_retired_aggregates(aggregate_id) SELECT DISTINCT aggregate_id FROM outcome_journal WHERE event_id=ANY($1::uuid[]) ON CONFLICT DO NOTHING`, journalIDs); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `DELETE FROM learning_outcome_outbox WHERE id=ANY($1::uuid[])`, journalIDs); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `DELETE FROM outcome_journal WHERE event_id=ANY($1::uuid[])`, journalIDs); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `DELETE FROM code_submission_outbox WHERE aggregate_id=ANY($1::uuid[])`, codeIDs); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `DELETE FROM code_submissions WHERE id=ANY($1::uuid[])`, codeIDs); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `DELETE FROM submission_answers WHERE submission_id=ANY($1::uuid[])`, choiceIDs); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `DELETE FROM submissions WHERE id=ANY($1::uuid[])`, choiceIDs); err != nil {
			return err
		}
	}
	if changed {
		if _, err = tx.Exec(ctx, `UPDATE outcome_journal_clock SET privacy_epoch=privacy_epoch+1 WHERE singleton`); err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

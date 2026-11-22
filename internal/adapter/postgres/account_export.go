package postgresadapter

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/overmindv/tasks/internal/domain"
)

const exportPageBytes = 2 << 20

func (r *Postgres) AccountExport(ctx context.Context, userID uuid.UUID, cursor, watermark string, limit int) (domain.AccountExportPage, error) {
	result := domain.AccountExportPage{UserID: userID, Records: []domain.AccountExportRecord{}}
	if userID == uuid.Nil || limit < 1 || limit > 100 {
		return result, domain.ErrOutcomeSnapshot
	}
	var after int64
	var err error
	if cursor != "" {
		after, err = strconv.ParseInt(cursor, 10, 64)
		if err != nil || after < 0 || watermark == "" {
			return result, domain.ErrOutcomeSnapshot
		}
	}
	var snapshot uuid.UUID
	if watermark == "" {
		snapshot, err = r.makeAccountExport(ctx, userID)
	} else {
		snapshot, err = uuid.Parse(watermark)
	}
	if err != nil {
		return result, err
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return result, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var valid bool
	err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM task_export_snapshots s JOIN outcome_journal_clock c ON c.singleton AND c.privacy_epoch=s.privacy_epoch
 WHERE s.id=$1 AND s.user_id=$2 AND s.expires_at>clock_timestamp() AND NOT EXISTS(SELECT 1 FROM task_privacy_tombstones WHERE user_id=$2))`, snapshot, userID).Scan(&valid)
	if err != nil {
		return result, err
	}
	if !valid {
		return result, domain.ErrOutcomeSnapshot
	}
	result.Watermark = snapshot.String()
	rows, err := tx.Query(ctx, `SELECT position,record FROM task_export_records WHERE snapshot_id=$1 AND position>$2 ORDER BY position LIMIT $3`, snapshot, after, limit+1)
	if err != nil {
		return result, err
	}
	var last int64
	size := 0
	for rows.Next() {
		var position int64
		var raw []byte
		if err = rows.Scan(&position, &raw); err != nil {
			rows.Close()

			return result, err
		}
		var record domain.AccountExportRecord
		if err = json.Unmarshal(raw, &record); err != nil {
			rows.Close()

			return result, err
		}
		encoded, encodeErr := json.Marshal(record)
		if encodeErr != nil {
			rows.Close()

			return result, encodeErr
		}
		if len(result.Records) == limit || size+len(encoded)+1 > exportPageBytes-1024 {
			if len(result.Records) == 0 {
				rows.Close()

				return result, domain.ErrOutcomeSnapshot
			}
			result.NextCursor = strconv.FormatInt(last, 10)

			break
		}
		result.Records = append(result.Records, record)
		last = position
		size += len(encoded) + 1
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return result, err
	}

	return result, tx.Commit(ctx)
}

func (r *Postgres) makeAccountExport(ctx context.Context, userID uuid.UUID) (uuid.UUID, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return uuid.Nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('tasks-user:'||$1::uuid::text,0))`, userID); err != nil {
		return uuid.Nil, err
	}
	var deleted bool
	var epoch int64
	if err = tx.QueryRow(ctx, `SELECT privacy_epoch,EXISTS(SELECT 1 FROM task_privacy_tombstones WHERE user_id=$1) FROM outcome_journal_clock WHERE singleton`, userID).Scan(&epoch, &deleted); err != nil {
		return uuid.Nil, err
	}
	if deleted {
		return uuid.Nil, domain.ErrOutcomeSnapshot
	}
	var active int
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM task_export_snapshots WHERE user_id=$1`, userID).Scan(&active); err != nil {
		return uuid.Nil, err
	}
	if active >= 5 {
		return uuid.Nil, domain.ErrOutcomeSnapshot
	}
	// Фиксированный snapshot не меняется при позднем grading или редактировании материала.
	rows, err := tx.Query(ctx, `SELECT jsonb_build_object('user_id',$1::uuid,'id',id,'kind',kind,'occurred_at',at,'resource_id',resource,'title',title,'text',body,'result',result)
 FROM (
 SELECT id,'submission' AS kind,created_at AS at,task_id AS resource,'' AS title,COALESCE(source_code,'') AS body,COALESCE(verdict,status) AS result FROM code_submissions WHERE user_id=$1
 UNION ALL SELECT s.id,'attempt',s.created_at,s.task_id,'',COALESCE((SELECT string_agg(option_id::text,',' ORDER BY option_id) FROM submission_answers WHERE submission_id=s.id),''),s.verdict FROM submissions s WHERE s.user_id=$1
 UNION ALL SELECT id,'task_revision',created_at,task_id,title,statement,'version:'||version_number::text FROM task_versions WHERE created_by=$1
 UNION ALL SELECT id,'task_review',created_at,COALESCE(approved_task_id,id),'','',status FROM task_candidates WHERE reviewed_by=$1
 UNION ALL SELECT id,'task_review',created_at,submission_id,'','','execution_replay' FROM code_submission_replay_audit WHERE actor_id=$1
 ) own ORDER BY kind,id LIMIT 10001`, userID)
	if err != nil {
		return uuid.Nil, err
	}
	snapshot := uuid.New()
	records := [][]any{}
	size := 0
	for rows.Next() {
		var raw []byte
		if err = rows.Scan(&raw); err != nil {
			rows.Close()

			return uuid.Nil, err
		}
		size += len(raw)
		if size > 10<<20 || len(records) >= 10000 {
			rows.Close()

			return uuid.Nil, domain.ErrOutcomeSnapshot
		}
		records = append(records, []any{snapshot, int64(len(records) + 1), raw})
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return uuid.Nil, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO task_export_snapshots(id,user_id,privacy_epoch) VALUES($1,$2,$3)`, snapshot, userID, epoch); err != nil {
		return uuid.Nil, err
	}
	if _, err = tx.CopyFrom(ctx, pgx.Identifier{"task_export_records"}, []string{"snapshot_id", "position", "record"}, pgx.CopyFromRows(records)); err != nil {
		return uuid.Nil, err
	}

	return snapshot, tx.Commit(ctx)
}

func (r *Postgres) ExpireAccountExports(ctx context.Context) error {
	_, err := r.query.Exec(ctx, `DELETE FROM task_export_snapshots WHERE id IN(SELECT s.id FROM task_export_snapshots s JOIN outcome_journal_clock c ON c.singleton WHERE s.expires_at<=clock_timestamp() OR s.privacy_epoch<>c.privacy_epoch ORDER BY s.id LIMIT 10)`)

	return err
}

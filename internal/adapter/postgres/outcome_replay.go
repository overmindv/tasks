package postgresadapter

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/jackc/pgx/v5"
	"github.com/overmindv/tasks/internal/domain"
)

func (r *Postgres) outcomeSnapshot(ctx context.Context, watermark, epoch int64) (pgx.Tx, domain.OutcomeManifest, error) {
	var manifest domain.OutcomeManifest
	if watermark < 0 || epoch < 0 {
		return nil, manifest, domain.ErrOutcomeSnapshot
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, manifest, err
	}
	err = tx.QueryRow(ctx, `SELECT position,privacy_epoch,legacy_missing,complete_since FROM outcome_journal_clock WHERE singleton`).Scan(&manifest.Watermark, &manifest.PrivacyEpoch, &manifest.LegacyMissing, &manifest.CompleteSince)
	if err != nil {
		_ = tx.Rollback(ctx)

		return nil, manifest, err
	}
	if watermark > manifest.Watermark || (epoch != 0 && epoch != manifest.PrivacyEpoch) {
		_ = tx.Rollback(ctx)

		return nil, manifest, domain.ErrOutcomeSnapshot
	}
	if watermark != 0 {
		manifest.Watermark = watermark
	}

	return tx, manifest, nil
}

func (r *Postgres) OutcomeManifest(ctx context.Context, watermark, epoch int64) (domain.OutcomeManifest, error) {
	tx, result, err := r.outcomeSnapshot(ctx, watermark, epoch)
	if err != nil {
		return result, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `SELECT payload FROM outcome_journal j WHERE position<=$1 AND NOT EXISTS(SELECT 1 FROM task_privacy_tombstones WHERE user_id=j.learner_id) ORDER BY position`, result.Watermark)
	if err != nil {
		return result, err
	}
	digest := sha256.New()
	for rows.Next() {
		var raw []byte
		if err = rows.Scan(&raw); err != nil {
			rows.Close()

			return result, err
		}
		hash, hashErr := domain.OutcomeDigest(raw)
		if hashErr != nil {
			rows.Close()

			return result, hashErr
		}
		decoded, _ := hex.DecodeString(hash)
		_, _ = digest.Write(decoded)
		result.Count++
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return result, err
	}
	result.SHA256 = hex.EncodeToString(digest.Sum(nil))

	return result, tx.Commit(ctx)
}

func (r *Postgres) OutcomeReplay(ctx context.Context, watermark, epoch, after int64, limit int) (domain.OutcomeReplayPage, error) {
	result := domain.OutcomeReplayPage{Events: []domain.OutcomeReplayEntry{}}
	if epoch < 1 || after < 0 || after > watermark || limit < 1 || limit > 100 {
		return result, domain.ErrOutcomeSnapshot
	}
	tx, manifest, err := r.outcomeSnapshot(ctx, watermark, epoch)
	if err != nil {
		return result, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	manifest.Watermark = watermark
	result.Watermark, result.PrivacyEpoch, result.Through = watermark, manifest.PrivacyEpoch, watermark
	rows, err := tx.Query(ctx, `SELECT position,payload FROM outcome_journal j WHERE position>$1 AND position<=$2 AND NOT EXISTS(SELECT 1 FROM task_privacy_tombstones WHERE user_id=j.learner_id) ORDER BY position LIMIT $3`, after, manifest.Watermark, limit+1)
	if err != nil {
		return result, err
	}
	for rows.Next() {
		var entry domain.OutcomeReplayEntry
		var raw []byte
		if err = rows.Scan(&entry.Position, &raw); err != nil {
			rows.Close()

			return result, err
		}
		if len(result.Events) == limit {
			result.More = true
			result.Through = result.Events[limit-1].Position

			break
		}
		if err = json.Unmarshal(raw, &entry.Event); err != nil {
			rows.Close()

			return result, err
		}
		entry.SHA256, err = domain.OutcomeDigest(raw)
		if err != nil {
			rows.Close()

			return result, err
		}
		result.Events = append(result.Events, entry)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return result, err
	}

	return result, tx.Commit(ctx)
}

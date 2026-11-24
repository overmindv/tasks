package postgresadapter

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/overmindv/tasks/internal/domain"
)

const taskActivityChangesQuery = `
	SELECT revision, event_id, occurred_at, task_id, task_version_id, event_type,
	       title, topic_id, task_type, difficulty
	FROM task_activity_changes
	WHERE revision > $1 AND revision <= $2
	ORDER BY revision
	LIMIT $3`

// TaskActivityWatermark возвращает зафиксированное значение часов журнала.
// Часы обновляются в транзакции смены статуса, поэтому номер совпадает
// с порядком коммитов и читатель не пропускает ещё не подтверждённые записи.
func (r *Postgres) TaskActivityWatermark(ctx context.Context) (int64, error) {
	var revision int64
	if err := r.query.QueryRow(ctx, `SELECT revision FROM task_activity_clock WHERE singleton`).Scan(&revision); err != nil {
		return 0, fmt.Errorf("read task activity watermark: %w", err)
	}

	return revision, nil
}

// TaskActivityChanges читает ограниченную страницу журнала публикаций.
func (r *Postgres) TaskActivityChanges(ctx context.Context, watermark, after int64, limit int) ([]domain.TaskActivityChange, error) {
	rows, err := r.query.Query(ctx, taskActivityChangesQuery, after, watermark, limit)
	if err != nil {
		return nil, fmt.Errorf("read task activity changes: %w", err)
	}
	changes, err := pgx.CollectRows(rows, scanTaskActivityChange)
	if err != nil {
		return nil, fmt.Errorf("decode task activity change: %w", err)
	}

	return changes, nil
}

func scanTaskActivityChange(row pgx.CollectableRow) (domain.TaskActivityChange, error) {
	var change domain.TaskActivityChange
	if err := row.Scan(
		&change.Revision,
		&change.EventID,
		&change.OccurredAt,
		&change.TaskID,
		&change.TaskVersionID,
		&change.EventType,
		&change.Title,
		&change.TopicID,
		&change.TaskType,
		&change.Difficulty,
	); err != nil {
		return change, err
	}

	return change, nil
}

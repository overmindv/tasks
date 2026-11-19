package postgresadapter

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/overmindv/tasks/internal/domain"
)

func (r *Postgres) ResourceWatermark(ctx context.Context) (int64, error) {
	var revision int64
	err := r.query.QueryRow(ctx, "SELECT revision FROM task_resource_clock WHERE singleton").Scan(&revision)
	if err != nil {
		return 0, fmt.Errorf("read resource watermark: %w", err)
	}

	return revision, nil
}

func (r *Postgres) ResourceSnapshot(ctx context.Context, watermark int64, after uuid.UUID, limit int) ([]domain.ResourceChange, error) {
	const query = `
		WITH page AS (
			SELECT task_id FROM task_resource_index
			WHERE task_id > $1 AND first_revision <= $2
			ORDER BY task_id LIMIT $3
		)
		SELECT c.event_id, c.task_id, c.revision, c.occurred_at, c.task_version_id,
		       c.topic_id, c.title, c.task_type, c.difficulty, c.estimated_minutes,
		       c.language_id, c.language_version
		FROM page AS p
		JOIN LATERAL (
			SELECT * FROM task_resource_changes
			WHERE task_id = p.task_id AND revision <= $2
			ORDER BY revision DESC LIMIT 1
		) AS c ON true
		ORDER BY p.task_id`

	return r.readResourceChanges(ctx, query, after, watermark, limit)
}

func (r *Postgres) ResourceChanges(ctx context.Context, watermark, after int64, limit int) ([]domain.ResourceChange, error) {
	const query = `
		SELECT event_id, task_id, revision, occurred_at, task_version_id, topic_id, title,
		       task_type, difficulty, estimated_minutes, language_id, language_version
		FROM task_resource_changes WHERE revision > $1 AND revision <= $2
		ORDER BY revision LIMIT $3`

	return r.readResourceChanges(ctx, query, after, watermark, limit)
}

func (r *Postgres) readResourceChanges(ctx context.Context, query string, args ...any) ([]domain.ResourceChange, error) {
	rows, err := r.query.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("read learning resources: %w", err)
	}
	changes, err := pgx.CollectRows(rows, scanResourceChange)
	if err != nil {
		return nil, fmt.Errorf("decode learning resource: %w", err)
	}

	return changes, nil
}

func scanResourceChange(row pgx.CollectableRow) (domain.ResourceChange, error) {
	var change domain.ResourceChange
	var versionID, topicID *uuid.UUID
	var title, taskType, difficulty, language, languageVersion *string
	var minutes *int
	if err := row.Scan(&change.EventID, &change.TaskID, &change.Revision, &change.OccurredAt, &versionID,
		&topicID, &title, &taskType, &difficulty, &minutes, &language, &languageVersion); err != nil {
		return change, err
	}
	if versionID != nil {
		change.Metadata = &domain.ResourceMetadata{
			VersionID:        *versionID,
			TopicID:          *topicID,
			Title:            *title,
			TaskType:         domain.TaskType(*taskType),
			Difficulty:       domain.Difficulty(*difficulty),
			EstimatedMinutes: minutes,
			Runtime:          resourceRuntime(language, languageVersion),
		}
	}

	return change, nil
}

func resourceRuntime(language, version *string) *domain.ResourceRuntime {
	if language == nil {
		return nil
	}

	return &domain.ResourceRuntime{Language: domain.ProgrammingLanguage(*language), Version: *version}
}

func (r *Postgres) AvailableResourceVersions(ctx context.Context, keys []domain.ResourceKey) ([]domain.ResourceAvailability, error) {
	return r.availableResourceVersions(ctx, keys, uuid.Nil)
}

func (r *Postgres) DiagnosticResourceVersions(ctx context.Context, learner uuid.UUID, keys []domain.ResourceKey) ([]domain.ResourceAvailability, error) {
	return r.availableResourceVersions(ctx, keys, learner)
}

func (r *Postgres) availableResourceVersions(ctx context.Context, keys []domain.ResourceKey, learner uuid.UUID) ([]domain.ResourceAvailability, error) {
	taskIDs := make([]uuid.UUID, len(keys))
	versionIDs := make([]uuid.UUID, len(keys))
	for i, key := range keys {
		taskIDs[i], versionIDs[i] = key.TaskID, key.VersionID
	}
	query := `
		SELECT t.id, t.current_version_id, c.language_id, c.language_version
		FROM unnest($1::uuid[], $2::uuid[]) AS wanted(task_id, version_id)
		JOIN tasks AS t ON t.id = wanted.task_id AND t.current_version_id = wanted.version_id
		JOIN LATERAL (
			SELECT task_version_id, language_id, language_version FROM task_resource_changes
			WHERE task_id = t.id ORDER BY revision DESC LIMIT 1
		) AS c ON c.task_version_id = t.current_version_id
		WHERE t.status = 'published' AND t.deleted_at IS NULL`
	args := []any{taskIDs, versionIDs}
	if learner != uuid.Nil {
		// Новая версия не делает уже знакомую задачу независимой проверкой.
		// Порядок попыток совпадает с индексами, чтобы не читать историю целиком.
		query += `
		  AND t.created_by <> $3
		  AND (
		      SELECT 1 FROM task_versions AS v
		      WHERE v.task_id = t.id AND v.created_by = $3
		      LIMIT 1
		  ) IS NULL
		  AND (
		      SELECT 1 FROM submissions AS s WHERE s.user_id = $3 AND s.task_id = t.id
		      ORDER BY s.created_at DESC, s.id DESC LIMIT 1
		  ) IS NULL
		  AND (
		      SELECT 1 FROM code_submissions AS s WHERE s.user_id = $3 AND s.task_id = t.id
		      ORDER BY s.created_at DESC, s.id DESC LIMIT 1
		  ) IS NULL`
		args = append(args, learner)
	}
	rows, err := r.query.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("check resource versions: %w", err)
	}
	result, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (domain.ResourceAvailability, error) {
		var availability domain.ResourceAvailability
		var language, version *string
		if err := row.Scan(&availability.Key.TaskID, &availability.Key.VersionID, &language, &version); err != nil {
			return availability, err
		}
		availability.Runtime = resourceRuntime(language, version)

		return availability, nil
	})
	if err != nil {
		return nil, fmt.Errorf("decode resource availability: %w", err)
	}

	return result, nil
}

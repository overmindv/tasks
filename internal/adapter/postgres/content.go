package postgresadapter

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/domain"
)

// InsertTaskContent сохраняет теги, примеры, ограничения и атрибуцию версии.
func (r *Postgres) InsertTaskContent(ctx context.Context, version domain.TaskVersion) error {
	for position, tag := range version.Tags {
		if _, err := r.query.Exec(ctx, `INSERT INTO task_version_tags (task_version_id, tag, position) VALUES ($1, $2, $3)`, version.ID, tag, position); err != nil {
			return fmt.Errorf("insert task version tag: %w", err)
		}
	}
	for position, example := range version.Examples {
		if _, err := r.query.Exec(ctx, `INSERT INTO task_version_examples (id, task_version_id, input, output, explanation, position) VALUES ($1, $2, $3, $4, $5, $6)`, uuid.New(), version.ID, example.Input, example.Output, example.Explanation, position); err != nil {
			return fmt.Errorf("insert task version example: %w", err)
		}
	}
	for position, constraint := range version.Constraints {
		if _, err := r.query.Exec(ctx, `INSERT INTO task_version_constraints (task_version_id, value, position) VALUES ($1, $2, $3)`, version.ID, constraint, position); err != nil {
			return fmt.Errorf("insert task version constraint: %w", err)
		}
	}
	if version.Source != nil {
		if _, err := r.query.Exec(ctx, `INSERT INTO task_version_sources (task_version_id, source_id, source_name, source_url, published_at) VALUES ($1, $2, $3, $4, $5)`, version.ID, version.Source.SourceID, version.Source.SourceName, version.Source.SourceURL, version.Source.PublishedAt); err != nil {
			return fmt.Errorf("insert task version source: %w", err)
		}
	}

	return nil
}

// hydrateTaskContent загружает дополнительное содержимое нескольких версий без N+1 запросов.
func (r *Postgres) hydrateTaskContent(ctx context.Context, versions map[uuid.UUID]*domain.TaskVersion) error {
	if len(versions) == 0 {
		return nil
	}
	ids := make([]uuid.UUID, 0, len(versions))
	for id := range versions {
		ids = append(ids, id)
	}
	rows, err := r.query.Query(ctx, `SELECT task_version_id, tag FROM task_version_tags WHERE task_version_id = ANY($1) ORDER BY task_version_id, position`, ids)
	if err != nil {
		return fmt.Errorf("query task version tags: %w", err)
	}
	for rows.Next() {
		var id uuid.UUID
		var value string
		if err := rows.Scan(&id, &value); err != nil {
			rows.Close()
			return fmt.Errorf("scan task version tag: %w", err)
		}
		versions[id].Tags = append(versions[id].Tags, value)
	}
	if err := rows.Err(); err != nil {
		rows.Close()

		return fmt.Errorf("iterate task version tags: %w", err)
	}
	rows.Close()

	rows, err = r.query.Query(ctx, `SELECT task_version_id, input, output, explanation FROM task_version_examples WHERE task_version_id = ANY($1) ORDER BY task_version_id, position`, ids)
	if err != nil {
		return fmt.Errorf("query task version examples: %w", err)
	}
	for rows.Next() {
		var id uuid.UUID
		var example domain.TaskExample
		if err := rows.Scan(&id, &example.Input, &example.Output, &example.Explanation); err != nil {
			rows.Close()
			return fmt.Errorf("scan task version example: %w", err)
		}
		versions[id].Examples = append(versions[id].Examples, example)
	}
	if err := rows.Err(); err != nil {
		rows.Close()

		return fmt.Errorf("iterate task version examples: %w", err)
	}
	rows.Close()

	rows, err = r.query.Query(ctx, `SELECT task_version_id, value FROM task_version_constraints WHERE task_version_id = ANY($1) ORDER BY task_version_id, position`, ids)
	if err != nil {
		return fmt.Errorf("query task version constraints: %w", err)
	}
	for rows.Next() {
		var id uuid.UUID
		var value string
		if err := rows.Scan(&id, &value); err != nil {
			rows.Close()
			return fmt.Errorf("scan task version constraint: %w", err)
		}
		versions[id].Constraints = append(versions[id].Constraints, value)
	}
	if err := rows.Err(); err != nil {
		rows.Close()

		return fmt.Errorf("iterate task version constraints: %w", err)
	}
	rows.Close()

	rows, err = r.query.Query(ctx, `SELECT task_version_id, source_id, source_name, source_url, published_at FROM task_version_sources WHERE task_version_id = ANY($1)`, ids)
	if err != nil {
		return fmt.Errorf("query task version sources: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id uuid.UUID
		var source domain.TaskSource
		if err := rows.Scan(&id, &source.SourceID, &source.SourceName, &source.SourceURL, &source.PublishedAt); err != nil {
			return fmt.Errorf("scan task version source: %w", err)
		}
		versions[id].Source = &source
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate task version sources: %w", err)
	}

	return nil
}

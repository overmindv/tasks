package postgresadapter

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/domain"
)

// GetProgressVisibility возвращает настройку только из tasks-owned БД.
func (r *Postgres) GetProgressVisibility(ctx context.Context, userID uuid.UUID) (bool, error) {
	var public bool
	err := r.query.QueryRow(ctx, `SELECT COALESCE((SELECT is_public FROM practice_progress_visibility WHERE user_id = $1), false)`, userID).Scan(&public)
	if err != nil {
		return false, fmt.Errorf("read practice progress visibility: %w", err)
	}
	return public, nil
}

func (r *Postgres) SetProgressVisibility(ctx context.Context, userID uuid.UUID, public bool) error {
	_, err := r.query.Exec(ctx, `
		INSERT INTO practice_progress_visibility (user_id, is_public) VALUES ($1, $2)
		ON CONFLICT (user_id) DO UPDATE SET is_public = EXCLUDED.is_public, updated_at = now()`, userID, public)
	if err != nil {
		return fmt.Errorf("save practice progress visibility: %w", err)
	}
	return nil
}

// VisitGradedAttempts читает всю историю потоково: статистика не зависит от
// пагинации UI и не держит исходный код, ответы или всю историю в памяти.
func (r *Postgres) VisitGradedAttempts(ctx context.Context, userID uuid.UUID, visit func(domain.GradedAttempt) error) error {
	rows, err := r.query.Query(ctx, `
		SELECT task_id, difficulty, verdict, occurred_at FROM (
			SELECT s.task_id, tv.difficulty, s.verdict, s.created_at AS occurred_at, s.id AS attempt_id
			FROM submissions s JOIN task_versions tv ON tv.id = s.task_version_id
			WHERE s.user_id = $1
			UNION ALL
			SELECT cs.task_id, tv.difficulty, cs.verdict, cs.completed_at AS occurred_at, cs.id AS attempt_id
			FROM code_submissions cs JOIN task_versions tv ON tv.id = cs.task_version_id
			WHERE cs.user_id = $1 AND cs.status = 'completed'
		) attempts ORDER BY occurred_at, attempt_id`, userID)
	if err != nil {
		return fmt.Errorf("query practice attempts: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var attempt domain.GradedAttempt
		if err := rows.Scan(&attempt.TaskID, &attempt.Difficulty, &attempt.Verdict, &attempt.OccurredAt); err != nil {
			return fmt.Errorf("scan practice attempt: %w", err)
		}
		if err := visit(attempt); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate practice attempts: %w", err)
	}
	return nil
}

// ListRatedUsers выдаёт кандидатов, отсортированных по очкам. Место здесь не
// назначается: gateway сначала исключит удалённые аккаунты через users API.
func (r *Postgres) ListRatedUsers(ctx context.Context, topicIDs *[]uuid.UUID, limit, offset int) ([]domain.RatedUser, error) {
	if topicIDs != nil && len(*topicIDs) == 0 {
		return []domain.RatedUser{}, nil
	}
	filter := ""
	args := []any{}
	if topicIDs != nil {
		filter = "WHERE tv.topic_id = ANY($1::uuid[])"
		args = append(args, *topicIDs)
	}
	limitPosition := len(args) + 1
	args = append(args, limit, offset)
	query := fmt.Sprintf(`
		WITH accepted AS (
			SELECT s.user_id, s.task_id, s.task_version_id, s.created_at AS occurred_at, s.id AS attempt_id
			FROM submissions s WHERE s.verdict = 'accepted'
			UNION ALL
			SELECT cs.user_id, cs.task_id, cs.task_version_id, cs.completed_at AS occurred_at, cs.id AS attempt_id
			FROM code_submissions cs WHERE cs.status = 'completed' AND cs.verdict = 'accepted'
		), first_accepted AS (
			SELECT DISTINCT ON (user_id, task_id) user_id, task_id, task_version_id
			FROM accepted ORDER BY user_id, task_id, occurred_at, attempt_id
		)
		SELECT first_accepted.user_id, COUNT(*)::int AS solved_tasks,
			SUM(CASE tv.difficulty WHEN 'easy' THEN %d WHEN 'medium' THEN %d WHEN 'hard' THEN %d ELSE 0 END)::int AS score
		FROM first_accepted
		JOIN task_versions tv ON tv.id = first_accepted.task_version_id
		JOIN practice_progress_visibility visibility ON visibility.user_id = first_accepted.user_id AND visibility.is_public
		%s
		GROUP BY first_accepted.user_id
		ORDER BY score DESC, first_accepted.user_id
		LIMIT $%d OFFSET $%d`, domain.RatingPoints("easy"), domain.RatingPoints("medium"), domain.RatingPoints("hard"), filter, limitPosition, limitPosition+1)
	rows, err := r.query.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query practice rating: %w", err)
	}
	defer rows.Close()
	items := make([]domain.RatedUser, 0, limit)
	for rows.Next() {
		var userID uuid.UUID
		var item domain.RatedUser
		if err := rows.Scan(&userID, &item.SolvedTasks, &item.Score); err != nil {
			return nil, fmt.Errorf("scan practice rating: %w", err)
		}
		item.UserID = userID.String()
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate practice rating: %w", err)
	}
	return items, nil
}

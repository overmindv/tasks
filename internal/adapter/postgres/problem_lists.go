package postgresadapter

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/overmindv/tasks/internal/apperror"
	"github.com/overmindv/tasks/internal/domain"
)

// GetOrCreateProblemList находит или создаёт список по source_url (идемпотентно).
// Вызывается внутри транзакции импорта кандидатов, поэтому доступен через tx-wrapper.
func (r *Postgres) GetOrCreateProblemList(ctx context.Context, input domain.ProblemListImport) (uuid.UUID, error) {
	var id uuid.UUID
	err := r.query.QueryRow(ctx, `SELECT id FROM problem_lists WHERE source_url=$1`, input.SourceURL).Scan(&id)
	if err == nil {
		return id, nil
	}
	if err != pgx.ErrNoRows {
		return uuid.Nil, fmt.Errorf("get problem list by source_url: %w", err)
	}

	id = uuid.New()
	if _, err := r.query.Exec(ctx, `INSERT INTO problem_lists (id, name, source_id, source_url) VALUES ($1,$2,$3,$4)`, id, input.Name, input.SourceID, input.SourceURL); err != nil {
		// Конкурентный сбор того же списка может выиграть уникальный ключ — перечитаем.
		if err := r.query.QueryRow(ctx, `SELECT id FROM problem_lists WHERE source_url=$1`, input.SourceURL).Scan(&id); err != nil {
			return uuid.Nil, fmt.Errorf("re-read problem list after conflict: %w", err)
		}

		return id, nil
	}

	return id, nil
}

// InsertProblemListMember добавляет опубликованную задачу в конец списка.
func (r *Postgres) InsertProblemListMember(ctx context.Context, problemListID, taskID uuid.UUID, _ int) error {
	if _, err := r.query.Exec(ctx, `INSERT INTO problem_list_members (problem_list_id, task_id, position)
VALUES ($1,$2, COALESCE((SELECT MAX(position)+1 FROM problem_list_members WHERE problem_list_id=$1), 0))
ON CONFLICT (problem_list_id, task_id) DO NOTHING`, problemListID, taskID); err != nil {
		return fmt.Errorf("insert problem list member: %w", err)
	}

	return nil
}

// ListProblemLists возвращает списки задач с числом опубликованных членов.
func (r *Postgres) ListProblemLists(ctx context.Context, limit, offset int) ([]domain.ProblemListSummary, error) {
	rows, err := r.query.Query(ctx, `SELECT pl.id, pl.name, pl.source_id, pl.source_url, pl.created_at, pl.updated_at,
COALESCE(count(pm.task_id), 0) AS member_count
FROM problem_lists pl
LEFT JOIN problem_list_members pm ON pm.problem_list_id = pl.id
GROUP BY pl.id
ORDER BY pl.created_at DESC, pl.id DESC
LIMIT $1 OFFSET $2`, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("query problem lists: %w", err)
	}
	defer rows.Close()

	items := make([]domain.ProblemListSummary, 0)
	for rows.Next() {
		var item domain.ProblemListSummary
		if err := rows.Scan(&item.ID, &item.Name, &item.SourceID, &item.SourceURL, &item.CreatedAt, &item.UpdatedAt, &item.MemberCount); err != nil {
			return nil, fmt.Errorf("scan problem list: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate problem lists: %w", err)
	}

	return items, nil
}

// GetProblemListDetail возвращает список с членами и числом ожидающих кандидатов.
func (r *Postgres) GetProblemListDetail(ctx context.Context, id uuid.UUID) (domain.ProblemListDetail, error) {
	var detail domain.ProblemListDetail
	err := r.query.QueryRow(ctx, `SELECT id, name, source_id, source_url, created_at, updated_at FROM problem_lists WHERE id=$1`, id).
		Scan(&detail.ID, &detail.Name, &detail.SourceID, &detail.SourceURL, &detail.CreatedAt, &detail.UpdatedAt)
	if err != nil {
		return domain.ProblemListDetail{}, notFound(err, apperror.ProblemListNotFound, "список задач не найден")
	}
	if err := r.query.QueryRow(ctx, `SELECT count(*) FROM problem_list_members WHERE problem_list_id=$1`, id).Scan(&detail.MemberCount); err != nil {
		return domain.ProblemListDetail{}, fmt.Errorf("count problem list members: %w", err)
	}
	if err := r.query.QueryRow(ctx, `SELECT count(*) FROM task_candidates WHERE problem_list_id=$1 AND status='pending'`, id).Scan(&detail.PendingCandidates); err != nil {
		return domain.ProblemListDetail{}, fmt.Errorf("count pending list candidates: %w", err)
	}

	detail.Members = make([]domain.ProblemListMember, 0, detail.MemberCount)
	rows, err := r.query.Query(ctx, `SELECT m.task_id, v.title, v.difficulty, t.status
FROM problem_list_members m
JOIN tasks t ON t.id = m.task_id
JOIN task_versions v ON v.id = t.current_version_id
WHERE m.problem_list_id = $1
ORDER BY m.position, m.task_id`, id)
	if err != nil {
		return domain.ProblemListDetail{}, fmt.Errorf("query problem list members: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var member domain.ProblemListMember
		var difficulty string
		var status string
		if err := rows.Scan(&member.TaskID, &member.Title, &difficulty, &status); err != nil {
			return domain.ProblemListDetail{}, fmt.Errorf("scan problem list member: %w", err)
		}
		member.Difficulty = domain.Difficulty(difficulty)
		member.Status = domain.TaskStatus(strings.ToLower(status))
		detail.Members = append(detail.Members, member)
	}
	if err := rows.Err(); err != nil {
		return domain.ProblemListDetail{}, fmt.Errorf("iterate problem list members: %w", err)
	}

	return detail, nil
}

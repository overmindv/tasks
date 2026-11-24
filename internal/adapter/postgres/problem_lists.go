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
		return r.syncProblemListTruncated(ctx, id, input.Truncated)
	}
	if err != pgx.ErrNoRows {
		return uuid.Nil, fmt.Errorf("get problem list by source_url: %w", err)
	}

	id = uuid.New()
	if _, err := r.query.Exec(ctx, `INSERT INTO problem_lists (id, name, source_id, source_url, truncated) VALUES ($1,$2,$3,$4,$5)`,
		id, input.Name, input.SourceID, input.SourceURL, input.Truncated); err != nil {
		// Конкурентный сбор того же списка может выиграть уникальный ключ — перечитаем.
		if err := r.query.QueryRow(ctx, `SELECT id FROM problem_lists WHERE source_url=$1`, input.SourceURL).Scan(&id); err != nil {
			return uuid.Nil, fmt.Errorf("re-read problem list after conflict: %w", err)
		}

		return r.syncProblemListTruncated(ctx, id, input.Truncated)
	}

	return id, nil
}

// syncProblemListTruncated поддерживает актуальный признак полноты списка:
// повторный полный сбор снимает отметку о частичном.
func (r *Postgres) syncProblemListTruncated(ctx context.Context, id uuid.UUID, truncated bool) (uuid.UUID, error) {
	if _, err := r.query.Exec(ctx, `UPDATE problem_lists SET truncated=$2, updated_at=now() WHERE id=$1 AND truncated<>$2`, id, truncated); err != nil {
		return uuid.Nil, fmt.Errorf("update problem list truncation: %w", err)
	}

	return id, nil
}

// InsertProblemListMember добавляет задачу в список на позицию сбора.
// nil position означает «в конец»: так обрабатываются кандидаты без порядка.
func (r *Postgres) InsertProblemListMember(ctx context.Context, problemListID, taskID uuid.UUID, position *int) error {
	if _, err := r.query.Exec(ctx, `INSERT INTO problem_list_members (problem_list_id, task_id, position)
VALUES ($1, $2, COALESCE($3::integer, (SELECT MAX(position)+1 FROM problem_list_members WHERE problem_list_id=$1), 0))
ON CONFLICT (problem_list_id, task_id) DO NOTHING`, problemListID, taskID, position); err != nil {
		return fmt.Errorf("insert problem list member: %w", err)
	}

	return nil
}

// ListProblemLists возвращает списки задач с числом опубликованных членов.
func (r *Postgres) ListProblemLists(ctx context.Context, limit, offset int, admin bool) ([]domain.ProblemListSummary, error) {
	memberCount := `COALESCE(count(pm.task_id), 0)`
	if !admin {
		memberCount = `COALESCE(count(pm.task_id) FILTER (WHERE t.status = 'published' AND t.deleted_at IS NULL), 0)`
	}
	rows, err := r.query.Query(ctx, `SELECT pl.id, pl.name, pl.source_id, pl.source_url, pl.truncated, pl.created_at, pl.updated_at,
`+memberCount+` AS member_count
FROM problem_lists pl
LEFT JOIN problem_list_members pm ON pm.problem_list_id = pl.id
LEFT JOIN tasks t ON t.id = pm.task_id
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
		if err := rows.Scan(&item.ID, &item.Name, &item.SourceID, &item.SourceURL, &item.Truncated, &item.CreatedAt, &item.UpdatedAt, &item.MemberCount); err != nil {
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
// Не-администратор видит только опубликованные задачи: черновики и архив
// не должны попадать в публичную выдачу.
func (r *Postgres) GetProblemListDetail(ctx context.Context, id uuid.UUID, admin bool) (domain.ProblemListDetail, error) {
	var detail domain.ProblemListDetail
	err := r.query.QueryRow(ctx, `SELECT id, name, source_id, source_url, truncated, created_at, updated_at FROM problem_lists WHERE id=$1`, id).
		Scan(&detail.ID, &detail.Name, &detail.SourceID, &detail.SourceURL, &detail.Truncated, &detail.CreatedAt, &detail.UpdatedAt)
	if err != nil {
		return domain.ProblemListDetail{}, notFound(err, apperror.ProblemListNotFound, "список задач не найден")
	}
	memberFilter := ""
	if !admin {
		memberFilter = ` AND t.status = 'published' AND t.deleted_at IS NULL`
	}
	if countErr := r.query.QueryRow(ctx, `SELECT count(*) FROM problem_list_members m
JOIN tasks t ON t.id = m.task_id
WHERE m.problem_list_id = $1`+memberFilter, id).Scan(&detail.MemberCount); countErr != nil {
		return domain.ProblemListDetail{}, fmt.Errorf("count problem list members: %w", countErr)
	}
	if countErr := r.query.QueryRow(ctx, `SELECT count(*) FROM task_candidates WHERE problem_list_id=$1 AND status='pending'`, id).Scan(&detail.PendingCandidates); countErr != nil {
		return domain.ProblemListDetail{}, fmt.Errorf("count pending list candidates: %w", countErr)
	}

	detail.Members = make([]domain.ProblemListMember, 0, detail.MemberCount)
	rows, err := r.query.Query(ctx, `SELECT m.task_id, v.title, v.difficulty, t.status
FROM problem_list_members m
JOIN tasks t ON t.id = m.task_id
JOIN task_versions v ON v.id = t.current_version_id
WHERE m.problem_list_id = $1`+memberFilter+`
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

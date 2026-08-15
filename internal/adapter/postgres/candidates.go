package postgresadapter

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/overmindv/tasks/internal/apperror"
	"github.com/overmindv/tasks/internal/domain"
)

const candidateColumns = `id, status, revision, external_id, source_id, source_name, source_url, source_hash,
source_published_at, retrieved_at, collection_job_id, topic_id, title, statement, difficulty,
approved_task_id, reviewed_by, reviewed_at, rejection_reason, created_at, updated_at`

type rowScanner interface {
	Scan(dest ...any) error
}

// InsertCandidate атомарно добавляет нового кандидата; дубликат возвращает false.
func (r *Postgres) InsertCandidate(ctx context.Context, candidate domain.TaskCandidate) (bool, error) {
	_, err := r.query.Exec(ctx, `INSERT INTO task_candidates (
id, status, revision, external_id, source_id, source_name, source_url, source_hash, source_published_at,
retrieved_at, collection_job_id, title, statement, difficulty
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`,
		candidate.ID, candidate.Status, candidate.Revision, candidate.ExternalID, candidate.SourceID,
		candidate.SourceName, candidate.SourceURL, candidate.SourceHash, candidate.SourcePublishedAt,
		candidate.RetrievedAt, candidate.CollectionJobID, candidate.Title, candidate.Statement, candidate.Difficulty)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return false, nil
		}

		return false, fmt.Errorf("insert task candidate: %w", err)
	}
	if err := r.replaceCandidateContent(ctx, candidate); err != nil {
		return false, err
	}

	return true, nil
}

// GetCandidate возвращает кандидата и при необходимости блокирует его строку.
func (r *Postgres) GetCandidate(ctx context.Context, id uuid.UUID, lock bool) (domain.TaskCandidate, error) {
	query := `SELECT ` + candidateColumns + ` FROM task_candidates WHERE id = $1`
	if lock {
		query += ` FOR UPDATE`
	}
	candidate, err := scanCandidate(r.query.QueryRow(ctx, query, id))
	if err != nil {
		return domain.TaskCandidate{}, notFound(err, apperror.CandidateNotFound, "кандидат не найден")
	}
	items := map[uuid.UUID]*domain.TaskCandidate{candidate.ID: &candidate}
	if err := r.hydrateCandidateContent(ctx, items); err != nil {
		return domain.TaskCandidate{}, fmt.Errorf("load candidate content: %w", err)
	}

	return candidate, nil
}

// ListCandidates возвращает очередь с явными фильтрами и стабильной пагинацией.
func (r *Postgres) ListCandidates(ctx context.Context, filter domain.CandidateFilter) ([]domain.TaskCandidate, error) {
	clauses := make([]string, 0, 3)
	args := make([]any, 0, 5)
	add := func(clause string, value any) {
		args = append(args, value)
		clauses = append(clauses, fmt.Sprintf(clause, len(args)))
	}
	if filter.Status != nil {
		add("status = $%d", *filter.Status)
	}
	if filter.SourceID != "" {
		add("source_id = $%d", filter.SourceID)
	}
	if filter.Difficulty != nil {
		add("difficulty = $%d", *filter.Difficulty)
	}
	query := `SELECT ` + candidateColumns + ` FROM task_candidates`
	if len(clauses) > 0 {
		query += ` WHERE ` + strings.Join(clauses, ` AND `)
	}
	args = append(args, filter.Limit, filter.Offset)
	query += fmt.Sprintf(` ORDER BY created_at DESC, id DESC LIMIT $%d OFFSET $%d`, len(args)-1, len(args))
	rows, err := r.query.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query task candidates: %w", err)
	}
	defer rows.Close()
	items := make([]domain.TaskCandidate, 0)
	for rows.Next() {
		candidate, scanErr := scanCandidate(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("scan task candidate: %w", scanErr)
		}
		items = append(items, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate task candidates: %w", err)
	}
	byID := make(map[uuid.UUID]*domain.TaskCandidate, len(items))
	for index := range items {
		byID[items[index].ID] = &items[index]
	}
	if err := r.hydrateCandidateContent(ctx, byID); err != nil {
		return nil, fmt.Errorf("load candidates content: %w", err)
	}

	return items, nil
}

// UpdateCandidate обновляет только pending-кандидата при совпавшей revision.
func (r *Postgres) UpdateCandidate(ctx context.Context, candidate domain.TaskCandidate, expectedRevision int) error {
	tag, err := r.query.Exec(ctx, `UPDATE task_candidates SET topic_id=$1, title=$2, statement=$3, difficulty=$4,
revision=revision+1, updated_at=now() WHERE id=$5 AND status='pending' AND revision=$6`, candidate.TopicID,
		candidate.Title, candidate.Statement, candidate.Difficulty, candidate.ID, expectedRevision)
	if err != nil {
		return fmt.Errorf("update task candidate: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return apperror.New(apperror.CandidateRevisionConflict, "кандидат изменён или уже обработан", http.StatusConflict)
	}
	if _, err := r.query.Exec(ctx, `DELETE FROM task_candidate_tags WHERE candidate_id=$1`, candidate.ID); err != nil {
		return fmt.Errorf("delete candidate tags: %w", err)
	}
	if _, err := r.query.Exec(ctx, `DELETE FROM task_candidate_examples WHERE candidate_id=$1`, candidate.ID); err != nil {
		return fmt.Errorf("delete candidate examples: %w", err)
	}
	if _, err := r.query.Exec(ctx, `DELETE FROM task_candidate_constraints WHERE candidate_id=$1`, candidate.ID); err != nil {
		return fmt.Errorf("delete candidate constraints: %w", err)
	}

	return r.replaceCandidateContent(ctx, candidate)
}

// MarkCandidateApproved завершает модерацию и связывает кандидата с опубликованной задачей.
func (r *Postgres) MarkCandidateApproved(ctx context.Context, candidateID, taskID, actorID uuid.UUID, expectedRevision int) error {
	tag, err := r.query.Exec(ctx, `UPDATE task_candidates SET status='approved', approved_task_id=$1, reviewed_by=$2,
reviewed_at=now(), revision=revision+1, updated_at=now() WHERE id=$3 AND status='pending' AND revision=$4`, taskID, actorID, candidateID, expectedRevision)
	if err != nil {
		return fmt.Errorf("mark candidate approved: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return apperror.New(apperror.CandidateRevisionConflict, "кандидат изменён или уже обработан", http.StatusConflict)
	}

	return nil
}

// MarkCandidateRejected завершает кандидата без создания задачи.
func (r *Postgres) MarkCandidateRejected(ctx context.Context, candidateID, actorID uuid.UUID, expectedRevision int, reason string) error {
	tag, err := r.query.Exec(ctx, `UPDATE task_candidates SET status='rejected', reviewed_by=$1, reviewed_at=now(),
rejection_reason=$2, revision=revision+1, updated_at=now() WHERE id=$3 AND status='pending' AND revision=$4`, actorID, reason, candidateID, expectedRevision)
	if err != nil {
		return fmt.Errorf("mark candidate rejected: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return apperror.New(apperror.CandidateRevisionConflict, "кандидат изменён или уже обработан", http.StatusConflict)
	}

	return nil
}

// replaceCandidateContent сохраняет дочерние поля кандидата после очистки прежних значений.
func (r *Postgres) replaceCandidateContent(ctx context.Context, candidate domain.TaskCandidate) error {
	for position, value := range candidate.Tags {
		if _, err := r.query.Exec(ctx, `INSERT INTO task_candidate_tags (candidate_id, tag, position) VALUES ($1,$2,$3)`, candidate.ID, value, position); err != nil {
			return fmt.Errorf("insert candidate tag: %w", err)
		}
	}
	for position, value := range candidate.Examples {
		if _, err := r.query.Exec(ctx, `INSERT INTO task_candidate_examples (id,candidate_id,input,output,explanation,position) VALUES ($1,$2,$3,$4,$5,$6)`, uuid.New(), candidate.ID, value.Input, value.Output, value.Explanation, position); err != nil {
			return fmt.Errorf("insert candidate example: %w", err)
		}
	}
	for position, value := range candidate.Constraints {
		if _, err := r.query.Exec(ctx, `INSERT INTO task_candidate_constraints (candidate_id,value,position) VALUES ($1,$2,$3)`, candidate.ID, value, position); err != nil {
			return fmt.Errorf("insert candidate constraint: %w", err)
		}
	}

	return nil
}

// hydrateCandidateContent загружает дочерние поля кандидатов тремя пакетными запросами.
func (r *Postgres) hydrateCandidateContent(ctx context.Context, candidates map[uuid.UUID]*domain.TaskCandidate) error {
	if len(candidates) == 0 {
		return nil
	}
	ids := make([]uuid.UUID, 0, len(candidates))
	for id := range candidates {
		ids = append(ids, id)
	}
	rows, err := r.query.Query(ctx, `SELECT candidate_id,tag FROM task_candidate_tags WHERE candidate_id=ANY($1) ORDER BY candidate_id,position`, ids)
	if err != nil {
		return fmt.Errorf("query candidate tags: %w", err)
	}
	for rows.Next() {
		var id uuid.UUID
		var value string
		if err := rows.Scan(&id, &value); err != nil {
			rows.Close()
			return fmt.Errorf("scan candidate tag: %w", err)
		}
		candidates[id].Tags = append(candidates[id].Tags, value)
	}
	if err := rows.Err(); err != nil {
		rows.Close()

		return fmt.Errorf("iterate candidate tags: %w", err)
	}
	rows.Close()
	rows, err = r.query.Query(ctx, `SELECT candidate_id,input,output,explanation FROM task_candidate_examples WHERE candidate_id=ANY($1) ORDER BY candidate_id,position`, ids)
	if err != nil {
		return fmt.Errorf("query candidate examples: %w", err)
	}
	for rows.Next() {
		var id uuid.UUID
		var value domain.TaskExample
		if err := rows.Scan(&id, &value.Input, &value.Output, &value.Explanation); err != nil {
			rows.Close()
			return fmt.Errorf("scan candidate example: %w", err)
		}
		candidates[id].Examples = append(candidates[id].Examples, value)
	}
	if err := rows.Err(); err != nil {
		rows.Close()

		return fmt.Errorf("iterate candidate examples: %w", err)
	}
	rows.Close()
	rows, err = r.query.Query(ctx, `SELECT candidate_id,value FROM task_candidate_constraints WHERE candidate_id=ANY($1) ORDER BY candidate_id,position`, ids)
	if err != nil {
		return fmt.Errorf("query candidate constraints: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id uuid.UUID
		var value string
		if err := rows.Scan(&id, &value); err != nil {
			return fmt.Errorf("scan candidate constraint: %w", err)
		}
		candidates[id].Constraints = append(candidates[id].Constraints, value)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate candidate constraints: %w", err)
	}

	return nil
}

// scanCandidate считывает основную строку кандидата.
func scanCandidate(row rowScanner) (domain.TaskCandidate, error) {
	var candidate domain.TaskCandidate
	var status string
	err := row.Scan(&candidate.ID, &status, &candidate.Revision, &candidate.ExternalID, &candidate.SourceID,
		&candidate.SourceName, &candidate.SourceURL, &candidate.SourceHash, &candidate.SourcePublishedAt,
		&candidate.RetrievedAt, &candidate.CollectionJobID, &candidate.TopicID, &candidate.Title,
		&candidate.Statement, &candidate.Difficulty, &candidate.ApprovedTaskID, &candidate.ReviewedBy,
		&candidate.ReviewedAt, &candidate.RejectionReason, &candidate.CreatedAt, &candidate.UpdatedAt)
	candidate.Status = domain.CandidateStatus(status)

	return candidate, err
}

package postgresadapter

import (
	"context"
	"fmt"

	jet "github.com/go-jet/jet/v2/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/overmindv/tasks/internal/apperror"
	"github.com/overmindv/tasks/internal/domain"
	"github.com/samber/lo"
)

// FindSubmissionByIdempotency ищет сохранённый результат по пользователю и ключу.
func (r *Postgres) FindSubmissionByIdempotency(ctx context.Context, userID, key uuid.UUID) (*domain.Submission, error) {
	condition := submissions.UserID.EQ(jet.UUID(userID)).
		AND(submissions.IdempotencyKey.EQ(jet.UUID(key)))
	items, err := r.querySubmissionBases(ctx, condition, 1, 0)
	if err != nil {
		return nil, fmt.Errorf("query submission by idempotency: %w", err)
	}
	if len(items) == 0 {
		return nil, nil
	}
	if err := r.hydrateSubmissions(ctx, items); err != nil {
		return nil, fmt.Errorf("hydrate idempotent submission: %w", err)
	}

	return &items[0], nil
}

// InsertSubmission сохраняет основной результат проверки ответа.
func (r *Postgres) InsertSubmission(ctx context.Context, submission domain.Submission) error {
	statement := submissions.INSERT(
		submissions.ID,
		submissions.UserID,
		submissions.TaskID,
		submissions.TaskVersionID,
		submissions.IdempotencyKey,
		submissions.RequestHash,
		submissions.Verdict,
	).VALUES(
		submission.ID.String(),
		submission.UserID.String(),
		submission.TaskID.String(),
		submission.TaskVersionID.String(),
		submission.IdempotencyKey.String(),
		submission.RequestHash,
		string(submission.Verdict),
	)
	query, args := statementSQL(statement)
	if _, err := r.query.Exec(ctx, query, args...); err != nil {
		return fmt.Errorf("execute insert submission: %w", err)
	}

	return nil
}

// InsertSubmissionAnswers сохраняет выбранные варианты одним Jet INSERT.
func (r *Postgres) InsertSubmissionAnswers(ctx context.Context, submissionID, versionID uuid.UUID, optionIDs []uuid.UUID) error {
	statement := submissionAnswers.INSERT(
		submissionAnswers.ID,
		submissionAnswers.SubmissionID,
		submissionAnswers.TaskVersionID,
		submissionAnswers.OptionID,
	)
	for _, optionID := range optionIDs {
		statement = statement.VALUES(uuid.New().String(), submissionID.String(), versionID.String(), optionID.String())
	}
	query, args := statementSQL(statement)
	if _, err := r.query.Exec(ctx, query, args...); err != nil {
		return fmt.Errorf("execute insert submission answers: %w", err)
	}

	return nil
}

// GetSubmission возвращает сохранённый результат с выбранными и правильными вариантами.
func (r *Postgres) GetSubmission(ctx context.Context, id uuid.UUID) (domain.Submission, error) {
	items, err := r.querySubmissionBases(ctx, submissions.ID.EQ(jet.UUID(id)), 1, 0)
	if err != nil {
		return domain.Submission{}, fmt.Errorf("query submission: %w", err)
	}
	if len(items) == 0 {
		return domain.Submission{}, apperror.New(apperror.SubmissionNotFound, "результат не найден", 404)
	}
	if err := r.hydrateSubmissions(ctx, items); err != nil {
		return domain.Submission{}, fmt.Errorf("hydrate submission: %w", err)
	}

	return items[0], nil
}

// ListSubmissions возвращает историю решений пользователя в стабильном порядке.
func (r *Postgres) ListSubmissions(ctx context.Context, filter domain.SubmissionFilter) ([]domain.Submission, error) {
	condition := submissions.UserID.EQ(jet.UUID(filter.UserID))
	if filter.TaskID != nil {
		condition = condition.AND(submissions.TaskID.EQ(jet.UUID(*filter.TaskID)))
	}
	items, err := r.querySubmissionBases(ctx, condition, filter.Limit, filter.Offset)
	if err != nil {
		return nil, fmt.Errorf("query submission history: %w", err)
	}
	if err := r.hydrateSubmissions(ctx, items); err != nil {
		return nil, fmt.Errorf("hydrate submission history: %w", err)
	}

	return items, nil
}

// querySubmissionBases читает результаты вместе с номерами отправленной и текущей версий.
func (r *Postgres) querySubmissionBases(ctx context.Context, condition jet.BoolExpression, limit, offset int) ([]domain.Submission, error) {
	submittedVersion := newTaskVersionsTable("submitted_version")
	currentVersion := newTaskVersionsTable("current_version")
	submissionTask := newTasksTable("submission_task")
	join := submissions.Table.
		INNER_JOIN(submittedVersion.Table, submissions.TaskVersionID.EQ(submittedVersion.ID)).
		INNER_JOIN(submissionTask.Table, submissions.TaskID.EQ(submissionTask.ID)).
		INNER_JOIN(currentVersion.Table, submissionTask.CurrentVersionID.EQ(currentVersion.ID))
	statement := jet.SELECT(
		submissions.ID,
		submissions.UserID,
		submissions.TaskID,
		submissions.TaskVersionID,
		submittedVersion.VersionNumber,
		submissions.IdempotencyKey,
		submissions.RequestHash,
		submissions.Verdict,
		currentVersion.ID,
		currentVersion.VersionNumber,
		submissions.CreatedAt,
		submissions.UpdatedAt,
	).FROM(join).WHERE(condition).
		ORDER_BY(submissions.CreatedAt.DESC(), submissions.ID.DESC()).
		LIMIT(int64(limit)).OFFSET(int64(offset))
	query, args := statementSQL(statement)
	rows, err := r.query.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("execute submission base query: %w", err)
	}
	defer rows.Close()
	items := make([]domain.Submission, 0)
	for rows.Next() {
		submission, scanErr := scanSubmissionBase(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("scan submission base: %w", scanErr)
		}
		items = append(items, submission)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate submission bases: %w", err)
	}

	return items, nil
}

// hydrateSubmissions добавляет к результатам выбранные и правильные варианты.
func (r *Postgres) hydrateSubmissions(ctx context.Context, items []domain.Submission) error {
	if len(items) == 0 {
		return nil
	}
	submissionIDs := lo.Map(items, func(item domain.Submission, _ int) uuid.UUID { return item.ID })
	selected, err := r.listSelectedOptions(ctx, submissionIDs)
	if err != nil {
		return fmt.Errorf("list selected options: %w", err)
	}
	versionIDs := lo.Uniq(lo.Map(items, func(item domain.Submission, _ int) uuid.UUID { return item.TaskVersionID }))
	options, err := r.listOptions(ctx, versionIDs)
	if err != nil {
		return fmt.Errorf("list correct options: %w", err)
	}
	for index := range items {
		items[index].SelectedOptionIDs = selected[items[index].ID]
		items[index].CorrectOptionIDs = lo.FilterMap(options[items[index].TaskVersionID], func(option domain.TaskOption, _ int) (uuid.UUID, bool) {
			return option.ID, option.IsCorrect
		})
		items[index].TaskUpdated = items[index].TaskVersionID != items[index].LatestTaskVersionID
	}

	return nil
}

// listSelectedOptions загружает выбранные варианты нескольких решений одним запросом.
func (r *Postgres) listSelectedOptions(ctx context.Context, submissionIDs []uuid.UUID) (map[uuid.UUID][]uuid.UUID, error) {
	result := make(map[uuid.UUID][]uuid.UUID, len(submissionIDs))
	statement := jet.SELECT(submissionAnswers.SubmissionID, submissionAnswers.OptionID).
		FROM(submissionAnswers.Table).
		WHERE(submissionAnswers.SubmissionID.IN(uuidExpressions(submissionIDs)...)).
		ORDER_BY(submissionAnswers.CreatedAt.ASC(), submissionAnswers.ID.ASC())
	query, args := statementSQL(statement)
	rows, err := r.query.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("execute selected options query: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var submissionID uuid.UUID
		var optionID uuid.UUID
		if err := rows.Scan(&submissionID, &optionID); err != nil {
			return nil, fmt.Errorf("scan selected option: %w", err)
		}
		result[submissionID] = append(result[submissionID], optionID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate selected options: %w", err)
	}

	return result, nil
}

// scanSubmissionBase считывает joined строку результата и версий.
func scanSubmissionBase(row pgx.Row) (domain.Submission, error) {
	var submission domain.Submission
	var verdict string
	err := row.Scan(
		&submission.ID,
		&submission.UserID,
		&submission.TaskID,
		&submission.TaskVersionID,
		&submission.TaskVersionNumber,
		&submission.IdempotencyKey,
		&submission.RequestHash,
		&verdict,
		&submission.LatestTaskVersionID,
		&submission.LatestVersionNumber,
		&submission.CreatedAt,
		&submission.UpdatedAt,
	)
	submission.Verdict = domain.Verdict(verdict)

	return submission, err
}

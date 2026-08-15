package postgresadapter

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/overmindv/tasks/internal/apperror"
	"github.com/overmindv/tasks/internal/domain"
)

const codeSubmissionColumns = `
    cs.id,
    cs.user_id,
    cs.task_id,
    cs.task_version_id,
    tv.version_number,
    cs.execution_id,
    cs.correlation_id,
    cs.idempotency_key,
    cs.request_hash,
    cs.language,
    cs.source_file_name,
    COALESCE(cs.source_code, ''),
    cs.status,
    cs.verdict,
    cs.compilation_result,
    cs.execution_result,
    cs.test_results,
    cs.error_code,
    cs.error_message,
    cs.created_at,
    cs.updated_at,
    cs.completed_at`

// FindCodeSubmissionByIdempotency ищет программное решение пользователя по ключу запроса.
func (r *Postgres) FindCodeSubmissionByIdempotency(ctx context.Context, userID, key uuid.UUID) (*domain.CodeSubmission, error) {
	query := `SELECT ` + codeSubmissionColumns + `
        FROM code_submissions cs
        INNER JOIN task_versions tv ON tv.id = cs.task_version_id
        WHERE cs.user_id = $1 AND cs.idempotency_key = $2`
	submission, err := scanCodeSubmission(r.query.QueryRow(ctx, query, userID, key))
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}

		return nil, fmt.Errorf("scan code submission by idempotency: %w", err)
	}

	return &submission, nil
}

// InsertCodeSubmission сохраняет решение до постановки события в outbox.
func (r *Postgres) InsertCodeSubmission(ctx context.Context, submission domain.CodeSubmission) error {
	_, err := r.query.Exec(ctx, `
        INSERT INTO code_submissions (
            id, user_id, task_id, task_version_id, execution_id, correlation_id,
            idempotency_key, request_hash, language, source_file_name, source_code, status
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
		submission.ID,
		submission.UserID,
		submission.TaskID,
		submission.TaskVersionID,
		submission.ExecutionID,
		submission.CorrelationID,
		submission.IdempotencyKey,
		submission.RequestHash,
		submission.Language,
		submission.SourceFileName,
		submission.SourceCode,
		submission.Status,
	)
	if err != nil {
		return fmt.Errorf("execute insert code submission: %w", err)
	}

	return nil
}

// GetCodeSubmission возвращает программное решение и полученный результат sandbox.
func (r *Postgres) GetCodeSubmission(ctx context.Context, id uuid.UUID) (domain.CodeSubmission, error) {
	query := `SELECT ` + codeSubmissionColumns + `
        FROM code_submissions cs
        INNER JOIN task_versions tv ON tv.id = cs.task_version_id
        WHERE cs.id = $1`
	submission, err := scanCodeSubmission(r.query.QueryRow(ctx, query, id))
	if err != nil {
		return domain.CodeSubmission{}, notFound(err, apperror.CodeSubmissionNotFound, "результат запуска не найден")
	}

	return submission, nil
}

// ListCodeSubmissions возвращает историю программных решений пользователя.
func (r *Postgres) ListCodeSubmissions(ctx context.Context, filter domain.CodeSubmissionFilter) ([]domain.CodeSubmission, error) {
	query := `SELECT ` + codeSubmissionColumns + `
        FROM code_submissions cs
        INNER JOIN task_versions tv ON tv.id = cs.task_version_id
        WHERE cs.user_id = $1 AND ($2::uuid IS NULL OR cs.task_id = $2)
        ORDER BY cs.created_at DESC, cs.id DESC
        LIMIT $3 OFFSET $4`
	rows, err := r.query.Query(ctx, query, filter.UserID, filter.TaskID, filter.Limit, filter.Offset)
	if err != nil {
		return nil, fmt.Errorf("query code submission history: %w", err)
	}
	defer rows.Close()
	items := make([]domain.CodeSubmission, 0)
	for rows.Next() {
		item, scanErr := scanCodeSubmission(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("scan code submission history: %w", scanErr)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate code submission history: %w", err)
	}

	return items, nil
}

// CompleteCodeSubmission атомарно сохраняет финальный результат для ожидающего запуска.
func (r *Postgres) CompleteCodeSubmission(ctx context.Context, result domain.CodeSubmission) (bool, error) {
	if result.Verdict == nil || result.CompletedAt == nil {
		return false, fmt.Errorf("complete code submission: verdict and completed_at are required")
	}
	var err error
	var compilation any
	if result.Compilation != nil {
		compilation, err = json.Marshal(result.Compilation)
		if err != nil {
			return false, fmt.Errorf("marshal compilation result: %w", err)
		}
	}
	var executionResult any
	if result.Execution != nil {
		executionResult, err = json.Marshal(result.Execution)
		if err != nil {
			return false, fmt.Errorf("marshal execution result: %w", err)
		}
	}
	tests, err := json.Marshal(result.Tests)
	if err != nil {
		return false, fmt.Errorf("marshal test results: %w", err)
	}
	var errorCode any
	var errorMessage any
	if result.Failure != nil {
		errorCode = result.Failure.Code
		errorMessage = result.Failure.Message
	}
	tag, err := r.query.Exec(ctx, `
        UPDATE code_submissions
        SET status = 'completed',
            verdict = $7,
            compilation_result = $8::jsonb,
            execution_result = $9::jsonb,
            test_results = $10::jsonb,
            error_code = $11,
            error_message = $12,
            completed_at = $13,
            updated_at = now()
        WHERE id = $1
          AND execution_id = $2
          AND correlation_id = $3
          AND task_id = $4
          AND task_version_id = $5
          AND status = $6`,
		result.ID,
		result.ExecutionID,
		result.CorrelationID,
		result.TaskID,
		result.TaskVersionID,
		domain.CodeSubmissionStatusQueued,
		*result.Verdict,
		compilation,
		executionResult,
		tests,
		errorCode,
		errorMessage,
		result.CompletedAt,
	)
	if err != nil {
		return false, fmt.Errorf("execute complete code submission: %w", err)
	}

	return tag.RowsAffected() == 1, nil
}

// scanCodeSubmission декодирует строку решения вместе с типизированным JSON-результатом.
func scanCodeSubmission(row pgx.Row) (domain.CodeSubmission, error) {
	var submission domain.CodeSubmission
	var verdict *string
	var compilation []byte
	var execution []byte
	var tests []byte
	var errorCode *string
	var errorMessage *string
	err := row.Scan(
		&submission.ID,
		&submission.UserID,
		&submission.TaskID,
		&submission.TaskVersionID,
		&submission.TaskVersionNumber,
		&submission.ExecutionID,
		&submission.CorrelationID,
		&submission.IdempotencyKey,
		&submission.RequestHash,
		&submission.Language,
		&submission.SourceFileName,
		&submission.SourceCode,
		&submission.Status,
		&verdict,
		&compilation,
		&execution,
		&tests,
		&errorCode,
		&errorMessage,
		&submission.CreatedAt,
		&submission.UpdatedAt,
		&submission.CompletedAt,
	)
	if err != nil {
		return domain.CodeSubmission{}, err
	}
	if verdict != nil {
		value := domain.ExecutionVerdict(*verdict)
		submission.Verdict = &value
	}
	if err := decodeNullableJSON(compilation, &submission.Compilation); err != nil {
		return domain.CodeSubmission{}, fmt.Errorf("decode compilation result: %w", err)
	}
	if err := decodeNullableJSON(execution, &submission.Execution); err != nil {
		return domain.CodeSubmission{}, fmt.Errorf("decode execution result: %w", err)
	}
	if len(tests) > 0 {
		if err := json.Unmarshal(tests, &submission.Tests); err != nil {
			return domain.CodeSubmission{}, fmt.Errorf("decode test results: %w", err)
		}
	}
	if errorCode != nil || errorMessage != nil {
		submission.Failure = &domain.ExecutionFailure{}
		if errorCode != nil {
			submission.Failure.Code = *errorCode
		}
		if errorMessage != nil {
			submission.Failure.Message = *errorMessage
		}
	}

	return submission, nil
}

// decodeNullableJSON декодирует nullable JSONB в типизированный указатель.
func decodeNullableJSON(payload []byte, target any) error {
	if len(payload) == 0 {
		return nil
	}
	if err := json.Unmarshal(payload, target); err != nil {
		return fmt.Errorf("unmarshal nullable json: %w", err)
	}

	return nil
}

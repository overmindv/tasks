package usecase

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/apperror"
	"github.com/overmindv/tasks/internal/domain"
	"github.com/overmindv/tasks/internal/execution"
	"github.com/overmindv/tasks/internal/repository"
)

// CodeExecutionPolicy задаёт topic и ограничения, которыми владеет tasks.
type CodeExecutionPolicy struct {
	RequestsTopic    string
	TimeLimit        time.Duration
	MemoryLimitBytes int64
}

// CodeSubmissionInput описывает один загруженный пользователем файл решения.
type CodeSubmissionInput struct {
	TaskVersionID  uuid.UUID
	IdempotencyKey uuid.UUID
	Language       domain.ProgrammingLanguage
	SourceFileName string
	SourceCode     string
}

// ExecutionMessageMetadata содержит только безопасные Kafka-метаданные входящего сообщения.
type ExecutionMessageMetadata struct {
	Topic         string
	Partition     int32
	Offset        int64
	PayloadSHA256 string
}

// CodeSubmissionService ставит запуски в outbox и принимает финальные результаты sandbox.
type CodeSubmissionService struct {
	repository repository.Repository
	policy     CodeExecutionPolicy
}

// NewCodeSubmissionService создаёт сервис асинхронной проверки программных решений.
func NewCodeSubmissionService(store repository.Repository, policy CodeExecutionPolicy) *CodeSubmissionService {
	return &CodeSubmissionService{
		repository: store,
		policy:     policy,
	}
}

// Submit валидирует файл и атомарно сохраняет решение вместе с Kafka outbox event.
func (s *CodeSubmissionService) Submit(ctx context.Context, taskID, userID uuid.UUID, input CodeSubmissionInput) (domain.CodeSubmission, error) {
	input.SourceFileName, input.SourceCode = domain.NormalizeSourceFile(input.SourceFileName, input.SourceCode)
	if err := domain.ValidateSourceFile(input.Language, input.SourceFileName, input.SourceCode); err != nil {
		return domain.CodeSubmission{}, apperror.New(apperror.InvalidSourceFile, err.Error(), http.StatusBadRequest)
	}
	if input.TaskVersionID == uuid.Nil || input.IdempotencyKey == uuid.Nil {
		return domain.CodeSubmission{}, apperror.New(apperror.ValidationError, "task_version_id и idempotency_key обязательны", http.StatusBadRequest)
	}
	hash := codeRequestHash(taskID, input)
	existing, err := s.repository.FindCodeSubmissionByIdempotency(ctx, userID, input.IdempotencyKey)
	if err != nil {
		return domain.CodeSubmission{}, fmt.Errorf("find code submission by idempotency key: %w", err)
	}
	if existing != nil {
		return sameIdempotentCodeSubmission(*existing, hash)
	}
	var submission domain.CodeSubmission
	err = s.repository.WithinTransaction(ctx, func(tx repository.Repository) error {
		task, err := tx.GetTask(ctx, taskID, true)
		if err != nil {
			return fmt.Errorf("get task for code submission: %w", err)
		}
		if task.Status != domain.TaskStatusPublished {
			return apperror.New(apperror.TaskNotAvailable, "задача недоступна для решения", http.StatusConflict)
		}
		version, err := tx.GetTaskVersion(ctx, taskID, input.TaskVersionID)
		if err != nil {
			return fmt.Errorf("get submitted programming task version: %w", err)
		}
		if version.TaskType != domain.TaskTypeProgramming {
			return apperror.New(apperror.TaskTypeNotSubmittable, "загрузка файла доступна только для programming-задач", http.StatusConflict)
		}
		tests := openExecutionTests(version.Examples)
		if len(tests) == 0 {
			return apperror.New(apperror.TaskNotExecutable, "у задачи нет открытых тестов с ожидаемым результатом", http.StatusConflict)
		}
		now := time.Now().UTC()
		submission = domain.CodeSubmission{
			ID:                uuid.New(),
			UserID:            userID,
			TaskID:            taskID,
			TaskVersionID:     version.ID,
			TaskVersionNumber: version.VersionNumber,
			ExecutionID:       uuid.New(),
			CorrelationID:     uuid.New(),
			IdempotencyKey:    input.IdempotencyKey,
			RequestHash:       hash,
			Language:          input.Language,
			SourceFileName:    input.SourceFileName,
			SourceCode:        input.SourceCode,
			Status:            domain.CodeSubmissionStatusQueued,
			Tests:             []domain.ExecutionTestResult{},
			CreatedAt:         now,
			UpdatedAt:         now,
		}
		event := execution.RequestEvent{
			EventID:       uuid.New(),
			EventType:     execution.RequestEventType,
			SchemaVersion: execution.SchemaVersion,
			OccurredAt:    now,
			CorrelationID: submission.CorrelationID,
			SubmissionID:  submission.ID,
			ExecutionID:   submission.ExecutionID,
			TaskID:        taskID,
			TaskVersionID: version.ID,
			Language:      input.Language,
			Source: execution.SourceFile{
				Name:    input.SourceFileName,
				Content: input.SourceCode,
			},
			Tests: tests,
			Limits: execution.ResourceLimits{
				TimeMS:      s.policy.TimeLimit.Milliseconds(),
				MemoryBytes: s.policy.MemoryLimitBytes,
			},
		}
		payload, err := execution.EncodeRequest(event)
		if err != nil {
			return fmt.Errorf("encode execution request: %w", err)
		}
		if err := tx.InsertCodeSubmission(ctx, submission); err != nil {
			return fmt.Errorf("insert code submission: %w", err)
		}
		if err := tx.InsertOutboxMessage(ctx, domain.OutboxMessage{
			ID:          event.EventID,
			AggregateID: submission.ID,
			Topic:       s.policy.RequestsTopic,
			MessageKey:  submission.ExecutionID.String(),
			Payload:     payload,
			AvailableAt: now,
			CreatedAt:   now,
		}); err != nil {
			return fmt.Errorf("insert execution request into outbox: %w", err)
		}

		return nil
	})
	if err != nil {
		existing, findErr := s.repository.FindCodeSubmissionByIdempotency(ctx, userID, input.IdempotencyKey)
		if findErr == nil && existing != nil {
			return sameIdempotentCodeSubmission(*existing, hash)
		}

		return domain.CodeSubmission{}, fmt.Errorf("submit code solution: %w", err)
	}

	return submission, nil
}

// Get возвращает состояние запуска владельцу или администратору.
func (s *CodeSubmissionService) Get(ctx context.Context, submissionID, actorID uuid.UUID, admin bool) (domain.CodeSubmission, error) {
	submission, err := s.repository.GetCodeSubmission(ctx, submissionID)
	if err != nil {
		return domain.CodeSubmission{}, fmt.Errorf("get code submission: %w", err)
	}
	if !admin && submission.UserID != actorID {
		return domain.CodeSubmission{}, apperror.New(apperror.PermissionDenied, "нет доступа к результату запуска", http.StatusForbidden)
	}

	return submission, nil
}

// ListMine возвращает пагинированную историю программных решений пользователя.
func (s *CodeSubmissionService) ListMine(ctx context.Context, filter domain.CodeSubmissionFilter) ([]domain.CodeSubmission, error) {
	filter.Limit, filter.Offset = domain.NormalizePagination(filter.Limit, filter.Offset)
	items, err := s.repository.ListCodeSubmissions(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("list user code submissions: %w", err)
	}

	return items, nil
}

// HandleResult дедуплицирует Kafka message и атомарно завершает ожидающее решение.
func (s *CodeSubmissionService) HandleResult(ctx context.Context, metadata ExecutionMessageMetadata, event execution.ResultEvent) error {
	record := executionInboxRecord(metadata, &event.EventID, execution.InboxStatusProcessed, "")
	err := s.repository.WithinTransaction(ctx, func(tx repository.Repository) error {
		inserted, err := tx.InsertExecutionInbox(ctx, record)
		if err != nil {
			return fmt.Errorf("insert processed result inbox: %w", err)
		}
		if !inserted {
			return nil
		}
		current, err := tx.GetCodeSubmission(ctx, event.SubmissionID)
		if err != nil {
			return apperror.New(apperror.ExecutionResultMismatch, "результат относится к неизвестному запуску", http.StatusConflict)
		}
		if !matchesExecutionResult(current, event) {
			return apperror.New(apperror.ExecutionResultMismatch, "идентификаторы результата не соответствуют запуску", http.StatusConflict)
		}
		if current.Status == domain.CodeSubmissionStatusCompleted {
			return nil
		}
		version, err := tx.GetTaskVersion(ctx, event.TaskID, event.TaskVersionID)
		if err != nil {
			return apperror.New(apperror.ExecutionResultMismatch, "версия задачи результата не найдена", http.StatusConflict)
		}
		if err := validateExecutionTests(version.Examples, event); err != nil {
			return apperror.New(apperror.ExecutionResultMismatch, err.Error(), http.StatusConflict)
		}
		completedAt := event.OccurredAt.UTC()
		verdict := event.Verdict
		updated, err := tx.CompleteCodeSubmission(ctx, domain.CodeSubmission{
			ID:            event.SubmissionID,
			TaskID:        event.TaskID,
			TaskVersionID: event.TaskVersionID,
			ExecutionID:   event.ExecutionID,
			CorrelationID: event.CorrelationID,
			Status:        domain.CodeSubmissionStatusCompleted,
			Verdict:       &verdict,
			Compilation:   event.Compilation,
			Execution:     event.Execution,
			Tests:         event.Tests,
			Failure:       event.Failure,
			CompletedAt:   &completedAt,
		})
		if err != nil {
			return fmt.Errorf("complete code submission: %w", err)
		}
		if !updated {
			return errors.New("ожидающее решение не было завершено")
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("handle execution result: %w", err)
	}

	return nil
}

// RejectResult сохраняет offset невалидного или несовместимого события без исходного payload.
func (s *CodeSubmissionService) RejectResult(ctx context.Context, metadata ExecutionMessageMetadata, eventID *uuid.UUID, errorCode string) error {
	record := executionInboxRecord(metadata, eventID, execution.InboxStatusRejected, errorCode)
	inserted, err := s.repository.InsertExecutionInbox(ctx, record)
	if err != nil {
		return fmt.Errorf("insert rejected result inbox: %w", err)
	}
	_ = inserted

	return nil
}

// IsPermanentResultError сообщает consumer'у, что mismatch нужно записать как rejected и подтвердить.
func IsPermanentResultError(err error) bool {
	var public *apperror.Error

	return errors.As(err, &public) && public.Code == apperror.ExecutionResultMismatch
}

// openExecutionTests преобразует открытые примеры версии в стабильные тесты Kafka-контракта.
func openExecutionTests(examples []domain.TaskExample) []execution.TestCase {
	tests := make([]execution.TestCase, 0, len(examples))
	for position, example := range examples {
		if example.Output == "" {
			continue
		}
		tests = append(tests, execution.TestCase{
			ID:             "open-" + strconv.Itoa(position+1),
			Visibility:     execution.TestVisibilityOpen,
			Input:          example.Input,
			ExpectedOutput: example.Output,
		})
	}

	return tests
}

// codeRequestHash строит fingerprint всех полей, влияющих на запуск.
func codeRequestHash(taskID uuid.UUID, input CodeSubmissionInput) string {
	value := strings.Join([]string{
		taskID.String(),
		input.TaskVersionID.String(),
		string(input.Language),
		input.SourceFileName,
		input.SourceCode,
	}, "\x00")
	sum := sha256.Sum256([]byte(value))

	return hex.EncodeToString(sum[:])
}

// sameIdempotentCodeSubmission возвращает прежний запуск либо конфликт ключа.
func sameIdempotentCodeSubmission(submission domain.CodeSubmission, hash string) (domain.CodeSubmission, error) {
	if submission.RequestHash != hash {
		return domain.CodeSubmission{}, apperror.New(apperror.IdempotencyKeyConflict, "ключ уже использован для другого файла решения", http.StatusConflict)
	}

	return submission, nil
}

// executionInboxRecord создаёт запись дедупликации из Kafka metadata.
func executionInboxRecord(metadata ExecutionMessageMetadata, eventID *uuid.UUID, status, errorCode string) domain.ExecutionInboxRecord {
	return domain.ExecutionInboxRecord{
		ID:            uuid.New(),
		EventID:       eventID,
		Topic:         metadata.Topic,
		Partition:     metadata.Partition,
		Offset:        metadata.Offset,
		PayloadSHA256: metadata.PayloadSHA256,
		Status:        status,
		ErrorCode:     errorCode,
	}
}

// matchesExecutionResult защищает решение от результата другого запуска или окружения.
func matchesExecutionResult(submission domain.CodeSubmission, event execution.ResultEvent) bool {
	return submission.ID == event.SubmissionID &&
		submission.ExecutionID == event.ExecutionID &&
		submission.CorrelationID == event.CorrelationID &&
		submission.TaskID == event.TaskID &&
		submission.TaskVersionID == event.TaskVersionID
}

// validateExecutionTests проверяет уникальность и принадлежность test ID открытым тестам версии.
func validateExecutionTests(examples []domain.TaskExample, event execution.ResultEvent) error {
	expected := openExecutionTests(examples)
	allowed := make(map[string]struct{}, len(expected))
	for _, test := range expected {
		allowed[test.ID] = struct{}{}
	}
	seen := make(map[string]struct{}, len(event.Tests))
	for _, result := range event.Tests {
		if _, ok := allowed[result.TestID]; !ok {
			return fmt.Errorf("результат содержит неизвестный test_id %q", result.TestID)
		}
		if _, ok := seen[result.TestID]; ok {
			return fmt.Errorf("результат повторяет test_id %q", result.TestID)
		}
		seen[result.TestID] = struct{}{}
	}
	if event.Verdict == domain.ExecutionVerdictAccepted {
		if len(seen) != len(expected) {
			return errors.New("accepted должен содержать результаты всех открытых тестов")
		}
		for _, result := range event.Tests {
			if result.Verdict != domain.ExecutionVerdictAccepted {
				return errors.New("accepted не может содержать непройденный тест")
			}
		}
	}

	return nil
}

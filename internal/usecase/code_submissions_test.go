package usecase

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/domain"
	"github.com/overmindv/tasks/internal/execution"
	"github.com/overmindv/tasks/internal/repository"
)

type codeSubmissionMemoryRepository struct {
	repository.Repository
	task        domain.Task
	version     domain.TaskVersion
	submissions map[uuid.UUID]domain.CodeSubmission
	outbox      []domain.OutboxMessage
	inbox       map[string]domain.ExecutionInboxRecord
}

// WithinTransaction выполняет callback над тестовым repository.
func (r *codeSubmissionMemoryRepository) WithinTransaction(ctx context.Context, fn func(repository.Repository) error) error {
	return fn(r)
}

// GetTask возвращает подготовленную programming-задачу.
func (r *codeSubmissionMemoryRepository) GetTask(_ context.Context, _ uuid.UUID, _ bool) (domain.Task, error) {
	return r.task, nil
}

// GetTaskVersion возвращает подготовленную версию задачи.
func (r *codeSubmissionMemoryRepository) GetTaskVersion(_ context.Context, _, versionID uuid.UUID) (domain.TaskVersion, error) {
	if versionID != r.version.ID {
		return domain.TaskVersion{}, errors.New("version not found")
	}

	return r.version, nil
}

// FindCodeSubmissionByIdempotency ищет запуск по ключу пользователя.
func (r *codeSubmissionMemoryRepository) FindCodeSubmissionByIdempotency(_ context.Context, userID, key uuid.UUID) (*domain.CodeSubmission, error) {
	for _, submission := range r.submissions {
		if submission.UserID == userID && submission.IdempotencyKey == key {
			copy := submission

			return &copy, nil
		}
	}

	return nil, nil
}

// InsertCodeSubmission сохраняет тестовый запуск.
func (r *codeSubmissionMemoryRepository) InsertCodeSubmission(_ context.Context, submission domain.CodeSubmission) error {
	if r.submissions == nil {
		r.submissions = make(map[uuid.UUID]domain.CodeSubmission)
	}
	r.submissions[submission.ID] = submission

	return nil
}

// GetCodeSubmission возвращает сохранённый тестовый запуск.
func (r *codeSubmissionMemoryRepository) GetCodeSubmission(_ context.Context, id uuid.UUID) (domain.CodeSubmission, error) {
	submission, ok := r.submissions[id]
	if !ok {
		return domain.CodeSubmission{}, errors.New("submission not found")
	}

	return submission, nil
}

// InsertOutboxMessage сохраняет request event для проверки контракта.
func (r *codeSubmissionMemoryRepository) InsertOutboxMessage(_ context.Context, message domain.OutboxMessage) error {
	r.outbox = append(r.outbox, message)

	return nil
}

// InsertExecutionInbox дедуплицирует Kafka offset и event ID.
func (r *codeSubmissionMemoryRepository) InsertExecutionInbox(_ context.Context, record domain.ExecutionInboxRecord) (bool, error) {
	if r.inbox == nil {
		r.inbox = make(map[string]domain.ExecutionInboxRecord)
	}
	key := record.Topic + ":" + strconv.FormatInt(int64(record.Partition), 10) + ":" + strconv.FormatInt(record.Offset, 10)
	if record.EventID != nil {
		key = record.EventID.String()
	}
	if _, ok := r.inbox[key]; ok {
		return false, nil
	}
	r.inbox[key] = record

	return true, nil
}

// CompleteCodeSubmission применяет финальный result event к тестовому запуску.
func (r *codeSubmissionMemoryRepository) CompleteCodeSubmission(_ context.Context, result domain.CodeSubmission) (bool, error) {
	current := r.submissions[result.ID]
	if current.Status != domain.CodeSubmissionStatusQueued {
		return false, nil
	}
	current.Status = domain.CodeSubmissionStatusCompleted
	current.Verdict = result.Verdict
	current.Compilation = result.Compilation
	current.Execution = result.Execution
	current.Tests = result.Tests
	current.Failure = result.Failure
	current.CompletedAt = result.CompletedAt
	r.submissions[result.ID] = current

	return true, nil
}

// TestCodeSubmissionServiceUsesTransactionalOutboxAndIdempotency проверяет постановку запуска и повтор запроса.
func TestCodeSubmissionServiceUsesTransactionalOutboxAndIdempotency(t *testing.T) {
	t.Parallel()
	repo, service, taskID, versionID := newCodeSubmissionService()
	userID := uuid.New()
	key := uuid.New()
	input := CodeSubmissionInput{
		TaskVersionID:  versionID,
		IdempotencyKey: key,
		Language:       domain.ProgrammingLanguagePython,
		SourceFileName: "/tmp/main.py",
		SourceCode:     "print(input())",
	}
	created, err := service.Submit(context.Background(), taskID, userID, input)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if created.Status != domain.CodeSubmissionStatusQueued || created.SourceFileName != "main.py" || len(repo.outbox) != 1 {
		t.Fatalf("created = %#v, outbox = %d", created, len(repo.outbox))
	}
	var event execution.RequestEvent
	if err := json.Unmarshal(repo.outbox[0].Payload, &event); err != nil {
		t.Fatalf("decode outbox request: %v", err)
	}
	if event.SubmissionID != created.ID || event.ExecutionID != created.ExecutionID || len(event.Tests) != 1 || event.Limits.TimeMS != 1000 {
		t.Fatalf("event = %#v", event)
	}
	repeated, err := service.Submit(context.Background(), taskID, userID, input)
	if err != nil || repeated.ID != created.ID || len(repo.outbox) != 1 {
		t.Fatalf("idempotent Submit() = %#v, %v, outbox = %d", repeated, err, len(repo.outbox))
	}
	input.SourceCode = "print(2)"
	if _, err := service.Submit(context.Background(), taskID, userID, input); err == nil {
		t.Fatal("Submit() должен отклонить повтор ключа с другим содержимым")
	}
}

// TestCodeSubmissionServiceCompletesAndDeduplicatesResult проверяет inbox и финальное состояние.
func TestCodeSubmissionServiceCompletesAndDeduplicatesResult(t *testing.T) {
	t.Parallel()
	repo, service, taskID, versionID := newCodeSubmissionService()
	created, err := service.Submit(context.Background(), taskID, uuid.New(), CodeSubmissionInput{
		TaskVersionID:  versionID,
		IdempotencyKey: uuid.New(),
		Language:       domain.ProgrammingLanguageGo,
		SourceFileName: "main.go",
		SourceCode:     "package main\nfunc main() {}",
	})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	event := execution.ResultEvent{
		EventID:       uuid.New(),
		EventType:     execution.ResultEventType,
		SchemaVersion: execution.SchemaVersion,
		OccurredAt:    time.Now().UTC(),
		CorrelationID: created.CorrelationID,
		SubmissionID:  created.ID,
		ExecutionID:   created.ExecutionID,
		TaskID:        created.TaskID,
		TaskVersionID: created.TaskVersionID,
		Verdict:       domain.ExecutionVerdictAccepted,
		Tests: []domain.ExecutionTestResult{
			{TestID: "open-1", Verdict: domain.ExecutionVerdictAccepted},
		},
	}
	metadata := ExecutionMessageMetadata{Topic: "results", Partition: 0, Offset: 1, PayloadSHA256: strings.Repeat("a", 64)}
	if err := service.HandleResult(context.Background(), metadata, event); err != nil {
		t.Fatalf("HandleResult() error = %v", err)
	}
	if err := service.HandleResult(context.Background(), metadata, event); err != nil {
		t.Fatalf("duplicate HandleResult() error = %v", err)
	}
	completed := repo.submissions[created.ID]
	if completed.Status != domain.CodeSubmissionStatusCompleted || completed.Verdict == nil || *completed.Verdict != domain.ExecutionVerdictAccepted || len(repo.inbox) != 1 {
		t.Fatalf("completed = %#v, inbox = %d", completed, len(repo.inbox))
	}
}

// TestValidateExecutionTestsRejectsInconsistentAccepted проверяет защиту от ложного accepted.
func TestValidateExecutionTestsRejectsInconsistentAccepted(t *testing.T) {
	t.Parallel()
	examples := []domain.TaskExample{
		{Input: "1", Output: "1"},
		{Input: "2", Output: "2"},
	}
	event := execution.ResultEvent{
		Verdict: domain.ExecutionVerdictAccepted,
		Tests: []domain.ExecutionTestResult{
			{TestID: "open-1", Verdict: domain.ExecutionVerdictAccepted},
		},
	}
	if err := validateExecutionTests(examples, event); err == nil {
		t.Fatal("accepted должен содержать все открытые тесты")
	}
	event.Tests = []domain.ExecutionTestResult{
		{TestID: "open-1", Verdict: domain.ExecutionVerdictAccepted},
		{TestID: "hidden-1", Verdict: domain.ExecutionVerdictAccepted},
	}
	if err := validateExecutionTests(examples, event); err == nil {
		t.Fatal("результат должен отклонить неизвестный test_id")
	}
}

// newCodeSubmissionService создаёт опубликованную задачу с одним открытым тестом.
func newCodeSubmissionService() (*codeSubmissionMemoryRepository, *CodeSubmissionService, uuid.UUID, uuid.UUID) {
	taskID := uuid.New()
	versionID := uuid.New()
	repo := &codeSubmissionMemoryRepository{
		task: domain.Task{
			ID:               taskID,
			CurrentVersionID: &versionID,
			Status:           domain.TaskStatusPublished,
		},
		version: domain.TaskVersion{
			ID:            versionID,
			TaskID:        taskID,
			VersionNumber: 1,
			TaskType:      domain.TaskTypeProgramming,
			Examples: []domain.TaskExample{
				{Input: "hello", Output: "hello"},
			},
		},
	}
	service := NewCodeSubmissionService(repo, CodeExecutionPolicy{
		RequestsTopic:    "code-execution.requests.v1",
		TimeLimit:        time.Second,
		MemoryLimitBytes: 64 * 1024 * 1024,
	})

	return repo, service, taskID, versionID
}

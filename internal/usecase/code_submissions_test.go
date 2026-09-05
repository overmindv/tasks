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
	var request execution.RunRequest
	if err := json.Unmarshal(repo.outbox[0].Payload, &request); err != nil {
		t.Fatalf("decode outbox request: %v", err)
	}
	if request.SubmissionID != created.ID.String() || request.AttemptID != created.ExecutionID.String() ||
		request.Execution == nil || len(request.Execution.Inputs) != 1 ||
		request.Execution.Inputs[0] != "hello" ||
		request.Code.Files == nil || len(request.Code.Files) != 1 || request.Code.Files[0].ContentB64 == "" ||
		request.Limits.CPUms != 1000 || request.Limits.Wallms < request.Limits.CPUms {
		t.Fatalf("request = %#v", request)
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

// TestCodeSubmissionServiceConsoleDerivesFileName проверяет консольный вариант:
// имя файла выводится из языка и попадает в запуск sandbox.
func TestCodeSubmissionServiceConsoleDerivesFileName(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		language     domain.ProgrammingLanguage
		expectedFile string
	}{
		{name: "python", language: domain.ProgrammingLanguagePython, expectedFile: "solution.py"},
		{name: "go", language: domain.ProgrammingLanguageGo, expectedFile: "main.go"},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			repo, service, taskID, versionID := newCodeSubmissionService()
			created, err := service.Submit(context.Background(), taskID, uuid.New(), CodeSubmissionInput{
				TaskVersionID:  versionID,
				IdempotencyKey: uuid.New(),
				Language:       tc.language,
				SourceFileName: "", // консольный вариант: файл не загружен
				SourceCode:     "code",
			})
			if err != nil {
				t.Fatalf("Submit() error = %v", err)
			}
			if created.SourceFileName != tc.expectedFile {
				t.Fatalf("SourceFileName = %q, want %q", created.SourceFileName, tc.expectedFile)
			}
			var request execution.RunRequest
			if err := json.Unmarshal(repo.outbox[0].Payload, &request); err != nil {
				t.Fatalf("decode outbox request: %v", err)
			}
			if request.Code.Entrypoint != tc.expectedFile || len(request.Code.Files) != 1 || request.Code.Files[0].Path != tc.expectedFile {
				t.Fatalf("request.Code = %#v", request.Code)
			}
		})
	}
}

// TestCodeSubmissionServiceConsoleRejectsUnsupportedLanguage проверяет, что консольный вариант
// с неподдерживаемым языком и без имени файла отклоняется.
func TestCodeSubmissionServiceConsoleRejectsUnsupportedLanguage(t *testing.T) {
	t.Parallel()
	_, service, taskID, versionID := newCodeSubmissionService()
	_, err := service.Submit(context.Background(), taskID, uuid.New(), CodeSubmissionInput{
		TaskVersionID:  versionID,
		IdempotencyKey: uuid.New(),
		Language:       "javascript",
		SourceFileName: "",
		SourceCode:     "console.log(1)",
	})
	if err == nil {
		t.Fatal("Submit() должен отклонить неподдерживаемый язык консольного решения")
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
	exit := 0
	result := execution.RunResult{
		SchemaVersion: execution.ResultSchemaVersion,
		SubmissionID:  created.ID.String(),
		AttemptID:     created.ExecutionID.String(),
		UserID:        created.UserID.String(),
		TaskID:        created.TaskID.String(),
		Status:        execution.ResultStatusOK,
		Summary:       execution.ResultSummary{CasesTotal: 1},
		Resources:     execution.ResultResources{CPUms: 10, MemoryPeakBytes: 1024, ExitCode: &exit},
		Cases: []execution.CaseRunResult{
			{Index: 0, Stdout: "hello", Status: execution.CaseStatusOK, CPUms: 10, MemoryPeakBytes: 1024},
		},
		CreatedAt: time.Now().UTC(),
	}
	metadata := ExecutionMessageMetadata{Topic: "results", Partition: 0, Offset: 1, PayloadSHA256: strings.Repeat("a", 64)}
	if err := service.HandleRunResult(context.Background(), metadata, result); err != nil {
		t.Fatalf("HandleRunResult() error = %v", err)
	}
	if err := service.HandleRunResult(context.Background(), metadata, result); err != nil {
		t.Fatalf("duplicate HandleRunResult() error = %v", err)
	}
	completed := repo.submissions[created.ID]
	if completed.Status != domain.CodeSubmissionStatusCompleted || completed.Verdict == nil || *completed.Verdict != domain.ExecutionVerdictAccepted || len(repo.inbox) != 1 {
		t.Fatalf("completed = %#v, inbox = %d", completed, len(repo.inbox))
	}
}

// TestVerdictFromRunResult проверяет вывод вердикта из вывода кейсов в tasks.
func TestVerdictFromRunResult(t *testing.T) {
	t.Parallel()
	expected := []domain.TaskExample{
		{Input: "1", Output: "1"},
		{Input: "2", Output: "2"},
	}

	accepted := execution.RunResult{
		Status: execution.ResultStatusOK,
		Cases: []execution.CaseRunResult{
			{Index: 0, Stdout: "1", Status: execution.CaseStatusOK},
			{Index: 1, Stdout: "2", Status: execution.CaseStatusOK},
		},
	}
	verdict, tests, _ := verdictFromRunResult(accepted, expected)
	if verdict != domain.ExecutionVerdictAccepted || len(tests) != 2 || tests[0].TestID != "open-1" {
		t.Fatalf("accepted verdict = %#v, tests = %#v", verdict, tests)
	}

	wrong := accepted
	wrong.Cases[1].Stdout = "3"
	verdict, _, _ = verdictFromRunResult(wrong, expected)
	if verdict != domain.ExecutionVerdictWrongAnswer {
		t.Fatalf("expected wrong_answer, got %q", verdict)
	}

	tle := accepted
	tle.Cases[0] = execution.CaseRunResult{Index: 0, Status: execution.ResultStatusTimeLimit}
	verdict, _, _ = verdictFromRunResult(tle, expected)
	if verdict != domain.ExecutionVerdictTimeLimitExceeded {
		t.Fatalf("expected time_limit_exceeded, got %q", verdict)
	}

	runLevel := execution.RunResult{Status: execution.ResultStatusRuntimeError, Error: &execution.RunError{Code: "runtime_error"}}
	verdict, _, _ = verdictFromRunResult(runLevel, expected)
	if verdict != domain.ExecutionVerdictRuntimeError {
		t.Fatalf("expected runtime_error, got %q", verdict)
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
		PIDs:             32,
		StdoutBytes:      32768,
		StderrBytes:      32768,
		WorkspaceBytes:   1048576,
		LanguageVersions: map[domain.ProgrammingLanguage]string{
			domain.ProgrammingLanguagePython: "3.12",
			domain.ProgrammingLanguageGo:     "1.26",
		},
	})

	return repo, service, taskID, versionID
}

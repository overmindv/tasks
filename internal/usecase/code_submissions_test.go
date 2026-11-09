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
	// failComplete симулирует конкурентное завершение запуска (raсе между
	// чтением и UPDATE), чтобы проверить permanent-ветку HandleRunResult.
	failComplete bool
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
	if current.Status != domain.CodeSubmissionStatusQueued || r.failComplete {
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

// BumpCodeSubmissionAttempt эмулирует guarded UPDATE попыток переотправки.
func (r *codeSubmissionMemoryRepository) BumpCodeSubmissionAttempt(_ context.Context, id uuid.UUID, maxAttempts int, nextAttemptAt time.Time) (bool, error) {
	current, ok := r.submissions[id]
	if !ok || current.Status != domain.CodeSubmissionStatusQueued || current.Attempts >= maxAttempts {
		return false, nil
	}
	current.Attempts++
	current.NextAttemptAt = nextAttemptAt
	current.UpdatedAt = time.Now().UTC()
	r.submissions[id] = current

	return true, nil
}

// FindExpiredQueuedCodeSubmissions возвращает queued-запуски с истёкшим окном.
func (r *codeSubmissionMemoryRepository) FindExpiredQueuedCodeSubmissions(_ context.Context, limit int) ([]domain.CodeSubmission, error) {
	now := time.Now().UTC()
	items := make([]domain.CodeSubmission, 0, len(r.submissions))
	for _, submission := range r.submissions {
		if submission.Status == domain.CodeSubmissionStatusQueued && !submission.NextAttemptAt.After(now) {
			items = append(items, submission)
		}
	}

	return items, nil
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

	// B2: внутренняя ошибка песочницы на кейсе — infrastructure_error, а не wrong_answer.
	infra := accepted
	infra.Cases[0] = execution.CaseRunResult{Index: 0, Status: execution.ResultStatusInternalError}
	verdict, _, _ = verdictFromRunResult(infra, expected)
	if verdict != domain.ExecutionVerdictInfrastructureError {
		t.Fatalf("expected infrastructure_error, got %q", verdict)
	}

	// B2: infrastructure_error имеет приоритет над wrong_answer другого кейса
	// (wrong_answer выводится из несовпадения stdout на «ok»-кейсе).
	mixed := accepted
	mixed.Cases[0] = execution.CaseRunResult{Index: 0, Status: execution.ResultStatusInternalError}
	mixed.Cases[1] = execution.CaseRunResult{Index: 1, Status: execution.CaseStatusOK, Stdout: "x"}
	verdict, _, _ = verdictFromRunResult(mixed, expected)
	if verdict != domain.ExecutionVerdictInfrastructureError {
		t.Fatalf("expected infrastructure_error over wrong_answer, got %q", verdict)
	}
}

// TestCodeSubmissionServiceRetriesInfraErrorThenExhausts проверяет B3: инфраструктурная
// ошибка не завершает решение сразу — запуск переотправляется до MaxAttempts, после
// чего завершается как infrastructure_error (не помечая решение неверным).
func TestCodeSubmissionServiceRetriesInfraErrorThenExhausts(t *testing.T) {
	t.Parallel()
	repo, service, taskID, versionID := newCodeSubmissionService()
	created, err := service.Submit(context.Background(), taskID, uuid.New(), CodeSubmissionInput{
		TaskVersionID:  versionID,
		IdempotencyKey: uuid.New(),
		Language:       domain.ProgrammingLanguagePython,
		SourceFileName: "solution.py",
		SourceCode:     "print(input())",
	})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}

	infraResult := execution.RunResult{
		SchemaVersion: execution.ResultSchemaVersion,
		SubmissionID:  created.ID.String(),
		AttemptID:     created.ExecutionID.String(),
		UserID:        created.UserID.String(),
		TaskID:        created.TaskID.String(),
		Status:        execution.ResultStatusInternalError,
		Resources:     execution.ResultResources{CPUms: 10, MemoryPeakBytes: 1024},
		Error:         &execution.RunError{Code: "worker_crash", Message: "sandbox worker died", Retryable: true},
		CreatedAt:     time.Now().UTC(),
	}

	// Первые две инфра-ошибки переотправляют запуск (attempts 1→2→3).
	for i := 2; i <= 3; i++ {
		metadata := ExecutionMessageMetadata{Topic: "results", Partition: 0, Offset: int64(i), PayloadSHA256: strings.Repeat("a", 64)}
		if err := service.HandleRunResult(context.Background(), metadata, infraResult); err != nil {
			t.Fatalf("HandleRunResult(%d) error = %v", i, err)
		}
		state := repo.submissions[created.ID]
		if state.Status != domain.CodeSubmissionStatusQueued || state.Attempts != i || len(repo.outbox) != i {
			t.Fatalf("after attempt %d: state = %#v, outbox = %d", i, state, len(repo.outbox))
		}
	}

	// Третья (attempts == MaxAttempts) — завершаем infrastructure_error.
	metadata := ExecutionMessageMetadata{Topic: "results", Partition: 0, Offset: 4, PayloadSHA256: strings.Repeat("b", 64)}
	if err := service.HandleRunResult(context.Background(), metadata, infraResult); err != nil {
		t.Fatalf("HandleRunResult(final) error = %v", err)
	}
	finished := repo.submissions[created.ID]
	if finished.Status != domain.CodeSubmissionStatusCompleted {
		t.Fatalf("status = %q, want completed", finished.Status)
	}
	if finished.Verdict == nil || *finished.Verdict != domain.ExecutionVerdictInfrastructureError {
		t.Fatalf("verdict = %v, want infrastructure_error", finished.Verdict)
	}
	if len(repo.outbox) != 3 {
		t.Fatalf("outbox = %d, want 3 (1 первичный + 2 ретрая)", len(repo.outbox))
	}
}

// TestCodeSubmissionServiceReaperRequeuesOrCompletes проверяет B3-reaper: заброшенные
// queued-запуски переотправляются до лимита, затем завершаются как infrastructure_error.
func TestCodeSubmissionServiceReaperRequeuesOrCompletes(t *testing.T) {
	t.Parallel()
	repo, service, taskID, versionID := newCodeSubmissionService()
	created, err := service.Submit(context.Background(), taskID, uuid.New(), CodeSubmissionInput{
		TaskVersionID:  versionID,
		IdempotencyKey: uuid.New(),
		Language:       domain.ProgrammingLanguagePython,
		SourceFileName: "solution.py",
		SourceCode:     "print(input())",
	})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}

	// Запуск «заснул»: sandbox не ответил, окно следующей попытки истекло.
	state := repo.submissions[created.ID]
	state.NextAttemptAt = time.Now().UTC().Add(-time.Hour)
	repo.submissions[created.ID] = state

	if err := service.RequeueStaleCodeSubmissions(context.Background()); err != nil {
		t.Fatalf("RequeueStaleCodeSubmissions() error = %v", err)
	}
	state = repo.submissions[created.ID]
	if state.Status != domain.CodeSubmissionStatusQueued || state.Attempts != 2 || len(repo.outbox) != 2 {
		t.Fatalf("after reaper requeue: state = %#v, outbox = %d", state, len(repo.outbox))
	}

	// Снова истёкшее окно при attempts == MaxAttempts — reaper завершает запуск.
	state.NextAttemptAt = time.Now().UTC().Add(-time.Hour)
	state.Attempts = 3
	repo.submissions[created.ID] = state
	if err := service.RequeueStaleCodeSubmissions(context.Background()); err != nil {
		t.Fatalf("RequeueStaleCodeSubmissions() error = %v", err)
	}
	finished := repo.submissions[created.ID]
	if finished.Status != domain.CodeSubmissionStatusCompleted {
		t.Fatalf("status = %q, want completed", finished.Status)
	}
	if finished.Verdict == nil || *finished.Verdict != domain.ExecutionVerdictInfrastructureError {
		t.Fatalf("verdict = %v, want infrastructure_error", finished.Verdict)
	}
}

// TestCodeSubmissionServiceConcurrentCompletionIsPermanent проверяет B4: если запуск
// не удалось завершить (конкурентная обработка), ошибка должна быть permanent,
// чтобы consumer не ретраил poison-сообщение вечно.
func TestCodeSubmissionServiceConcurrentCompletionIsPermanent(t *testing.T) {
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
	repo.failComplete = true

	exit := 0
	result := execution.RunResult{
		SchemaVersion: execution.ResultSchemaVersion,
		SubmissionID:  created.ID.String(),
		AttemptID:     created.ExecutionID.String(),
		UserID:        created.UserID.String(),
		TaskID:        created.TaskID.String(),
		Status:        execution.ResultStatusOK,
		Resources:     execution.ResultResources{CPUms: 10, MemoryPeakBytes: 1024, ExitCode: &exit},
		Cases: []execution.CaseRunResult{
			{Index: 0, Stdout: "hello", Status: execution.CaseStatusOK, CPUms: 10, MemoryPeakBytes: 1024},
		},
		CreatedAt: time.Now().UTC(),
	}
	metadata := ExecutionMessageMetadata{Topic: "results", Partition: 0, Offset: 7, PayloadSHA256: strings.Repeat("c", 64)}
	err = service.HandleRunResult(context.Background(), metadata, result)
	if err == nil {
		t.Fatal("HandleRunResult() должен вернуть ошибку при конкурентном завершении")
	}
	if !IsPermanentResultError(err) {
		t.Fatalf("ошибка должна быть permanent (ExecutionResultMismatch), got: %v", err)
	}
}

// TestCodeSubmissionServiceRejectResult проверяет запись rejected inbox записи:
// невалидное или несовместимое событие фиксируется вместе с безопасными метаданными
// и event ID для дедупликации.
func TestCodeSubmissionServiceRejectResult(t *testing.T) {
	t.Parallel()
	repo, service, _, _ := newCodeSubmissionService()
	eventID := uuid.New()
	metadata := ExecutionMessageMetadata{Topic: "results", Partition: 0, Offset: 42, PayloadSHA256: strings.Repeat("d", 64)}
	if err := service.RejectResult(context.Background(), metadata, &eventID, execution.InboxErrorInvalidEvent); err != nil {
		t.Fatalf("RejectResult() error = %v", err)
	}
	if len(repo.inbox) != 1 {
		t.Fatalf("inbox = %d, want 1 rejected запись", len(repo.inbox))
	}
	var record domain.ExecutionInboxRecord
	for _, r := range repo.inbox {
		record = r
	}
	if record.Status != execution.InboxStatusRejected || record.ErrorCode != execution.InboxErrorInvalidEvent {
		t.Fatalf("record = %#v", record)
	}
	if record.EventID == nil || *record.EventID != eventID {
		t.Fatalf("event_id = %v, want %v", record.EventID, eventID)
	}
	if record.Topic != metadata.Topic || record.Partition != metadata.Partition ||
		record.Offset != metadata.Offset || record.PayloadSHA256 != metadata.PayloadSHA256 {
		t.Fatalf("metadata не сохранена в record = %#v", record)
	}
}

// TestCodeSubmissionServiceNonRetryableCheckerErrorCompletes проверяет, что
// checker_error без пометки retryable не переотправляется: решение завершается
// как checker_error без дополнительных outbox-событий.
func TestCodeSubmissionServiceNonRetryableCheckerErrorCompletes(t *testing.T) {
	t.Parallel()
	repo, service, taskID, versionID := newCodeSubmissionService()
	created, err := service.Submit(context.Background(), taskID, uuid.New(), CodeSubmissionInput{
		TaskVersionID:  versionID,
		IdempotencyKey: uuid.New(),
		Language:       domain.ProgrammingLanguagePython,
		SourceFileName: "solution.py",
		SourceCode:     "print(input())",
	})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	checkerResult := execution.RunResult{
		SchemaVersion: execution.ResultSchemaVersion,
		SubmissionID:  created.ID.String(),
		AttemptID:     created.ExecutionID.String(),
		UserID:        created.UserID.String(),
		TaskID:        created.TaskID.String(),
		Status:        execution.ResultStatusInvalidRequest,
		Resources:     execution.ResultResources{CPUms: 10, MemoryPeakBytes: 1024},
		Error:         &execution.RunError{Code: "invalid_request", Message: "входные данные не прошли валидацию", Retryable: false},
		CreatedAt:     time.Now().UTC(),
	}
	metadata := ExecutionMessageMetadata{Topic: "results", Partition: 0, Offset: 5, PayloadSHA256: strings.Repeat("e", 64)}
	if err := service.HandleRunResult(context.Background(), metadata, checkerResult); err != nil {
		t.Fatalf("HandleRunResult() error = %v", err)
	}
	finished := repo.submissions[created.ID]
	if finished.Status != domain.CodeSubmissionStatusCompleted {
		t.Fatalf("status = %q, want completed", finished.Status)
	}
	if finished.Verdict == nil || *finished.Verdict != domain.ExecutionVerdictCheckerError {
		t.Fatalf("verdict = %v, want checker_error", finished.Verdict)
	}
	if len(repo.outbox) != 1 {
		t.Fatalf("outbox = %d, want 1 (без ретрая)", len(repo.outbox))
	}
}

// TestCodeSubmissionServiceRetryableCheckerErrorRequeues проверяет обратную ветку
// codeSubmissionRetryable для checker_error: если песочница явно пометила ошибку
// retryable, запуск переотправляется, а не завершается.
func TestCodeSubmissionServiceRetryableCheckerErrorRequeues(t *testing.T) {
	t.Parallel()
	repo, service, taskID, versionID := newCodeSubmissionService()
	created, err := service.Submit(context.Background(), taskID, uuid.New(), CodeSubmissionInput{
		TaskVersionID:  versionID,
		IdempotencyKey: uuid.New(),
		Language:       domain.ProgrammingLanguagePython,
		SourceFileName: "solution.py",
		SourceCode:     "print(input())",
	})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	retryableResult := execution.RunResult{
		SchemaVersion: execution.ResultSchemaVersion,
		SubmissionID:  created.ID.String(),
		AttemptID:     created.ExecutionID.String(),
		UserID:        created.UserID.String(),
		TaskID:        created.TaskID.String(),
		Status:        execution.ResultStatusUnsupportedLang,
		Resources:     execution.ResultResources{CPUms: 10, MemoryPeakBytes: 1024},
		Error:         &execution.RunError{Code: "unsupported_language", Message: "рантайм временно недоступен", Retryable: true},
		CreatedAt:     time.Now().UTC(),
	}
	metadata := ExecutionMessageMetadata{Topic: "results", Partition: 0, Offset: 6, PayloadSHA256: strings.Repeat("g", 64)}
	if err := service.HandleRunResult(context.Background(), metadata, retryableResult); err != nil {
		t.Fatalf("HandleRunResult() error = %v", err)
	}
	state := repo.submissions[created.ID]
	if state.Status != domain.CodeSubmissionStatusQueued || state.Attempts != 2 || len(repo.outbox) != 2 {
		t.Fatalf("after requeue: status=%q attempts=%d outbox=%d", state.Status, state.Attempts, len(repo.outbox))
	}
}

// TestCodeSubmissionServiceUnknownResultIsPermanent проверяет, что ветки
// HandleRunResult, не связанные с запуском (некорректный или неизвестный
// submission_id), возвращают permanent ExecutionResultMismatch — его consumer
// превращает в rejected-запись и подтверждает offset.
func TestCodeSubmissionServiceUnknownResultIsPermanent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		make func() execution.RunResult
	}{
		{
			name: "invalid submission id",
			make: func() execution.RunResult {
				return execution.RunResult{
					SchemaVersion: execution.ResultSchemaVersion,
					SubmissionID:  "not-a-uuid",
					AttemptID:     uuid.NewString(),
					UserID:        uuid.NewString(),
					TaskID:        uuid.NewString(),
					Status:        execution.ResultStatusOK,
					Summary:       execution.ResultSummary{CasesTotal: 1},
					Resources:     execution.ResultResources{CPUms: 10, MemoryPeakBytes: 1024},
					Cases: []execution.CaseRunResult{
						{Index: 0, Stdout: "hello", Status: execution.CaseStatusOK},
					},
					CreatedAt: time.Now().UTC(),
				}
			},
		},
		{
			name: "unknown submission",
			make: func() execution.RunResult {
				return execution.RunResult{
					SchemaVersion: execution.ResultSchemaVersion,
					SubmissionID:  uuid.NewString(),
					AttemptID:     uuid.NewString(),
					UserID:        uuid.NewString(),
					TaskID:        uuid.NewString(),
					Status:        execution.ResultStatusOK,
					Summary:       execution.ResultSummary{CasesTotal: 1},
					Resources:     execution.ResultResources{CPUms: 10, MemoryPeakBytes: 1024},
					Cases: []execution.CaseRunResult{
						{Index: 0, Stdout: "hello", Status: execution.CaseStatusOK},
					},
					CreatedAt: time.Now().UTC(),
				}
			},
		},
	}
	for i, tc := range tests {
		i, tc := i, tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, service, _, _ := newCodeSubmissionService()
			metadata := ExecutionMessageMetadata{Topic: "results", Partition: 0, Offset: int64(10 + i), PayloadSHA256: strings.Repeat("h", 64)}
			err := service.HandleRunResult(context.Background(), metadata, tc.make())
			if err == nil {
				t.Fatal("HandleRunResult() должен вернуть ошибку для несоответствующего результата")
			}
			if !IsPermanentResultError(err) {
				t.Fatalf("ошибка должна быть permanent (ExecutionResultMismatch), got: %v", err)
			}
		})
	}
}

// TestCodeSubmissionServiceMismatchedAttemptIsPermanent проверяет permanent-ветку
// matchesRunResult: результат с корректным submission_id, но чужим attempt_id
// не должен ретраиться consumer-ом.
func TestCodeSubmissionServiceMismatchedAttemptIsPermanent(t *testing.T) {
	t.Parallel()
	_, service, taskID, versionID := newCodeSubmissionService()
	created, err := service.Submit(context.Background(), taskID, uuid.New(), CodeSubmissionInput{
		TaskVersionID:  versionID,
		IdempotencyKey: uuid.New(),
		Language:       domain.ProgrammingLanguagePython,
		SourceFileName: "solution.py",
		SourceCode:     "print(input())",
	})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	exit := 0
	result := execution.RunResult{
		SchemaVersion: execution.ResultSchemaVersion,
		SubmissionID:  created.ID.String(),
		AttemptID:     uuid.NewString(), // чужой attempt_id
		UserID:        created.UserID.String(),
		TaskID:        created.TaskID.String(),
		Status:        execution.ResultStatusOK,
		Summary:       execution.ResultSummary{CasesTotal: 1},
		Resources:     execution.ResultResources{CPUms: 10, MemoryPeakBytes: 1024, ExitCode: &exit},
		Cases: []execution.CaseRunResult{
			{Index: 0, Stdout: "hello", Status: execution.CaseStatusOK},
		},
		CreatedAt: time.Now().UTC(),
	}
	metadata := ExecutionMessageMetadata{Topic: "results", Partition: 0, Offset: 20, PayloadSHA256: strings.Repeat("j", 64)}
	err = service.HandleRunResult(context.Background(), metadata, result)
	if err == nil {
		t.Fatal("HandleRunResult() должен вернуть ошибку для чужого attempt_id")
	}
	if !IsPermanentResultError(err) {
		t.Fatalf("ошибка должна быть permanent (ExecutionResultMismatch), got: %v", err)
	}
}

// TestCodeSubmissionServiceRequeueWithoutExamplesIsPermanent проверяет permanent-ветку
// requeue: если у версии задачи больше нет открытых примеров для переотправки,
// HandleRunResult возвращает ExecutionResultMismatch, а не ретраит в бесконечном цикле.
func TestCodeSubmissionServiceRequeueWithoutExamplesIsPermanent(t *testing.T) {
	t.Parallel()
	repo, service, taskID, versionID := newCodeSubmissionService()
	created, err := service.Submit(context.Background(), taskID, uuid.New(), CodeSubmissionInput{
		TaskVersionID:  versionID,
		IdempotencyKey: uuid.New(),
		Language:       domain.ProgrammingLanguagePython,
		SourceFileName: "solution.py",
		SourceCode:     "print(input())",
	})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	// Версия «потеряла» открытые примеры с ожидаемым выводом после первичной отправки.
	repo.version.Examples = []domain.TaskExample{{Input: "1", Output: ""}}

	infraResult := execution.RunResult{
		SchemaVersion: execution.ResultSchemaVersion,
		SubmissionID:  created.ID.String(),
		AttemptID:     created.ExecutionID.String(),
		UserID:        created.UserID.String(),
		TaskID:        created.TaskID.String(),
		Status:        execution.ResultStatusInternalError,
		Resources:     execution.ResultResources{CPUms: 10, MemoryPeakBytes: 1024},
		Error:         &execution.RunError{Code: "worker_crash", Message: "sandbox worker died", Retryable: true},
		CreatedAt:     time.Now().UTC(),
	}
	metadata := ExecutionMessageMetadata{Topic: "results", Partition: 0, Offset: 8, PayloadSHA256: strings.Repeat("k", 64)}
	err = service.HandleRunResult(context.Background(), metadata, infraResult)
	if err == nil {
		t.Fatal("HandleRunResult() должен вернуть ошибку при невозможности переотправки")
	}
	if !IsPermanentResultError(err) {
		t.Fatalf("ошибка должна быть permanent (ExecutionResultMismatch), got: %v", err)
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
		MaxAttempts:      3,
		RetryGrace:       time.Minute,
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

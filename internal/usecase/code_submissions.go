package usecase

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
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

// Границы исполнения, совместимые с sandbox (DefaultValidationOptions).
const (
	minMemoryMB = 16
	maxMemoryMB = 1024
	minCPUms    = 100
	maxCPUms    = 30000
	maxWallms   = 60000
)

// CodeExecutionPolicy задаёт topic, ограничения и версии языков, которыми владеет tasks.
type CodeExecutionPolicy struct {
	RequestsTopic    string
	TimeLimit        time.Duration
	MemoryLimitBytes int64

	// MaxAttempts лимитирует переотправки запуска в sandbox (включая первичную).
	MaxAttempts int
	// RetryGrace — окно, после которого reaper считает queued-запуск заброшенным.
	RetryGrace time.Duration

	// Sandbox-совместимые лимиты исполнения.
	PIDs           int
	StdoutBytes    int
	StderrBytes    int
	WorkspaceBytes int

	// LanguageVersions задаёт версию рантайма по умолчанию на язык.
	LanguageVersions map[domain.ProgrammingLanguage]string
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
	if input.SourceFileName == "" {
		// Вариант «код в консоли»: имя файла выводится детерминированно из языка.
		name, nameErr := domain.DefaultSourceFileName(input.Language)
		if nameErr != nil {
			return domain.CodeSubmission{}, apperror.New(apperror.InvalidSourceFile, nameErr.Error(), http.StatusBadRequest)
		}
		input.SourceFileName = name
	}
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
		open := openExamples(version.Examples)
		if len(open) == 0 {
			return apperror.New(apperror.TaskNotExecutable, "у задачи нет открытых примеров с ожидаемым результатом", http.StatusConflict)
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
			Attempts:          1,
			NextAttemptAt:     now,
			Tests:             []domain.ExecutionTestResult{},
			CreatedAt:         now,
			UpdatedAt:         now,
		}
		request := s.buildRunRequest(submission, open, now)
		payload, err := execution.EncodeRunRequest(request)
		if err != nil {
			return fmt.Errorf("encode execution request: %w", err)
		}
		if err := tx.InsertCodeSubmission(ctx, submission); err != nil {
			return fmt.Errorf("insert code submission: %w", err)
		}
		if err := tx.InsertOutboxMessage(ctx, domain.OutboxMessage{
			ID:          uuid.New(),
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

// buildRunRequest собирает запрос на исполнение для уже сохранённого решения:
// используется и при Submit, и при переотправке после инфраструктурной ошибки.
func (s *CodeSubmissionService) buildRunRequest(submission domain.CodeSubmission, open []domain.TaskExample, now time.Time) execution.RunRequest {
	return execution.RunRequest{
		SchemaVersion: execution.RequestSchemaVersion,
		SubmissionID:  submission.ID.String(),
		AttemptID:     submission.ExecutionID.String(),
		UserID:        submission.UserID.String(),
		TaskID:        submission.TaskID.String(),
		Language: execution.Language{
			ID:      string(submission.Language),
			Version: languageVersion(s.policy, submission.Language),
		},
		Code: execution.Code{
			Entrypoint: submission.SourceFileName,
			Files: []execution.SourceFile{
				{
					Path:       submission.SourceFileName,
					ContentB64: base64.StdEncoding.EncodeToString([]byte(submission.SourceCode)),
				},
			},
		},
		Execution: &execution.ExecutionSpec{
			Mode:   execution.ExecutionMode,
			Inputs: exampleInputs(open),
		},
		Limits:    s.executionLimits(submission.Language),
		TraceID:   submission.CorrelationID.String(),
		CreatedAt: now,
	}
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

// HandleRunResult дедуплицирует Kafka message, сверяет вывод кейсов с эталоном
// и атомарно завершает ожидающее решение.
func (s *CodeSubmissionService) HandleRunResult(ctx context.Context, metadata ExecutionMessageMetadata, result execution.RunResult) error {
	record := executionInboxRecord(metadata, nil, execution.InboxStatusProcessed, "")
	err := s.repository.WithinTransaction(ctx, func(tx repository.Repository) error {
		inserted, err := tx.InsertExecutionInbox(ctx, record)
		if err != nil {
			return fmt.Errorf("insert processed result inbox: %w", err)
		}
		if !inserted {
			return nil
		}
		submissionID, parseErr := uuid.Parse(result.SubmissionID)
		if parseErr != nil {
			return apperror.New(apperror.ExecutionResultMismatch, "результат содержит некорректный submission_id", http.StatusConflict)
		}
		current, err := tx.GetCodeSubmission(ctx, submissionID)
		if err != nil {
			return apperror.New(apperror.ExecutionResultMismatch, "результат относится к неизвестному запуску", http.StatusConflict)
		}
		if !matchesRunResult(current, result) {
			return apperror.New(apperror.ExecutionResultMismatch, "идентификаторы результата не соответствуют запуску", http.StatusConflict)
		}
		if current.Status == domain.CodeSubmissionStatusCompleted {
			return nil
		}
		version, err := tx.GetTaskVersion(ctx, current.TaskID, current.TaskVersionID)
		if err != nil {
			return apperror.New(apperror.ExecutionResultMismatch, "версия задачи результата не найдена", http.StatusConflict)
		}
		verdict, tests, failure := verdictFromRunResult(result, openExamples(version.Examples))
		// Инфраструктурная ошибка песочницы — не приговор для пользователя: пока не
		// исчерпаны попытки, переотправляем запуск через outbox вместо завершения.
		if codeSubmissionRetryable(verdict, result) && current.Attempts < s.policy.MaxAttempts {
			return s.requeue(ctx, tx, current, version, time.Now().UTC())
		}
		completedAt := result.CreatedAt.UTC()
		executionPhase := executionPhaseFromResult(result)
		updated, err := tx.CompleteCodeSubmission(ctx, domain.CodeSubmission{
			ID:            current.ID,
			TaskID:        current.TaskID,
			TaskVersionID: current.TaskVersionID,
			ExecutionID:   current.ExecutionID,
			CorrelationID: current.CorrelationID,
			Status:        domain.CodeSubmissionStatusCompleted,
			Verdict:       &verdict,
			Execution:     executionPhase,
			Tests:         tests,
			Failure:       failure,
			CompletedAt:   &completedAt,
		})
		if err != nil {
			return fmt.Errorf("complete code submission: %w", err)
		}
		if !updated {
			// Конкурентное завершение другим обработчиком: результат устарел.
			// Permanent error — иначе consumer бесконечно ретраит poison-сообщение.
			return apperror.New(apperror.ExecutionResultMismatch, "запуск завершён конкурирующим обработчиком", http.StatusConflict)
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

// codeSubmissionRetryable говорит, стоит ли переотправлять запуск после ответа sandbox.
// Инфраструктурная ошибка по своей природе переотправляема; checker_error — только
// когда песочница явно пометила её как retryable.
func codeSubmissionRetryable(verdict domain.ExecutionVerdict, result execution.RunResult) bool {
	switch verdict {
	case domain.ExecutionVerdictInfrastructureError:
		return true
	case domain.ExecutionVerdictCheckerError:
		return result.Error != nil && result.Error.Retryable
	default:
		return false
	}
}

// requeue переотправляет запуск в sandbox в той же транзакции обработки результата:
// увеличивает счётчик попыток, откладывает окно reaper-а и ставит новый outbox-event.
// Возвращает permanent mismatch, если запуск завершён конкурирующим обработчиком.
func (s *CodeSubmissionService) requeue(ctx context.Context, tx repository.Repository, current domain.CodeSubmission, version domain.TaskVersion, at time.Time) error {
	open := openExamples(version.Examples)
	if len(open) == 0 {
		return apperror.New(apperror.ExecutionResultMismatch, "версия задачи не содержит открытых примеров для переотправки", http.StatusConflict)
	}
	bumped, err := tx.BumpCodeSubmissionAttempt(ctx, current.ID, s.policy.MaxAttempts, at.Add(s.policy.RetryGrace))
	if err != nil {
		return fmt.Errorf("bump code submission attempt: %w", err)
	}
	if !bumped {
		return apperror.New(apperror.ExecutionResultMismatch, "запуск завершён конкурирующим обработчиком", http.StatusConflict)
	}
	request := s.buildRunRequest(current, open, at)
	payload, err := execution.EncodeRunRequest(request)
	if err != nil {
		return fmt.Errorf("encode retry execution request: %w", err)
	}
	if err := tx.InsertOutboxMessage(ctx, domain.OutboxMessage{
		ID:          uuid.New(),
		AggregateID: current.ID,
		Topic:       s.policy.RequestsTopic,
		MessageKey:  current.ExecutionID.String(),
		Payload:     payload,
		AvailableAt: at,
		CreatedAt:   at,
	}); err != nil {
		return fmt.Errorf("insert retry execution request into outbox: %w", err)
	}

	return nil
}

// RequeueStaleCodeSubmissions обрабатывает queued-запуски, на которые sandbox так и не
// ответил: переотправляет их до MaxAttempts, затем завершает как infrastructure_error
// (решение пользователя не помечается неверным). Вызывается reaper-ом периодически.
func (s *CodeSubmissionService) RequeueStaleCodeSubmissions(ctx context.Context) error {
	const reaperBatchSize = 50

	stale, err := s.repository.FindExpiredQueuedCodeSubmissions(ctx, reaperBatchSize)
	if err != nil {
		return fmt.Errorf("find expired queued code submissions: %w", err)
	}
	for _, candidate := range stale {
		if err := s.requeueOrFailOne(ctx, candidate); err != nil {
			return err
		}
	}

	return nil
}

// requeueOrFailOne решает судьбу одного заброшенного запуска в отдельной транзакции:
// завершает при исчерпании попыток, иначе переотправляет.
func (s *CodeSubmissionService) requeueOrFailOne(ctx context.Context, stale domain.CodeSubmission) error {
	return s.repository.WithinTransaction(ctx, func(tx repository.Repository) error {
		current, err := tx.GetCodeSubmission(ctx, stale.ID)
		if err != nil {
			return fmt.Errorf("get stale code submission: %w", err)
		}
		if current.Status != domain.CodeSubmissionStatusQueued {
			return nil
		}
		if current.Attempts >= s.policy.MaxAttempts {
			verdict := domain.ExecutionVerdictInfrastructureError
			failure := &domain.ExecutionFailure{
				Code:    "attempts_exhausted",
				Message: "песочница не ответила за отведённое число попыток",
			}
			completedAt := time.Now().UTC()
			updated, err := tx.CompleteCodeSubmission(ctx, domain.CodeSubmission{
				ID:            current.ID,
				TaskID:        current.TaskID,
				TaskVersionID: current.TaskVersionID,
				ExecutionID:   current.ExecutionID,
				CorrelationID: current.CorrelationID,
				Status:        domain.CodeSubmissionStatusCompleted,
				Verdict:       &verdict,
				Tests:         []domain.ExecutionTestResult{},
				Failure:       failure,
				CompletedAt:   &completedAt,
			})
			if err != nil {
				return fmt.Errorf("complete stalled code submission: %w", err)
			}
			if !updated {
				// Конкурентно завершён (пришёл запоздавший результат) — не ошибка.
				return nil
			}

			return nil
		}
		version, err := tx.GetTaskVersion(ctx, current.TaskID, current.TaskVersionID)
		if err != nil {
			return fmt.Errorf("get stale code submission version: %w", err)
		}

		return s.requeue(ctx, tx, current, version, time.Now().UTC())
	})
}

// executionLimits строит sandbox-совместимые лимиты с учётом допустимых границ песочницы.
func (s *CodeSubmissionService) executionLimits(_ domain.ProgrammingLanguage) execution.Limits {
	memoryMB := int(s.policy.MemoryLimitBytes / (1 << 20))
	if memoryMB < minMemoryMB {
		memoryMB = minMemoryMB
	}
	if memoryMB > maxMemoryMB {
		memoryMB = maxMemoryMB
	}
	cpuMS := int(s.policy.TimeLimit.Milliseconds())
	if cpuMS < minCPUms {
		cpuMS = minCPUms
	}
	if cpuMS > maxCPUms {
		cpuMS = maxCPUms
	}
	wallMS := cpuMS
	if wallMS > maxWallms {
		wallMS = maxWallms
	}

	return execution.Limits{
		CPUms:          cpuMS,
		Wallms:         wallMS,
		MemoryMB:       memoryMB,
		PIDs:           s.policy.PIDs,
		StdoutBytes:    s.policy.StdoutBytes,
		StderrBytes:    s.policy.StderrBytes,
		WorkspaceBytes: s.policy.WorkspaceBytes,
	}
}

// languageVersion возвращает версию рантайма по умолчанию для языка решения.
func languageVersion(policy CodeExecutionPolicy, language domain.ProgrammingLanguage) string {
	if version := policy.LanguageVersions[language]; version != "" {
		return version
	}
	switch language {
	case domain.ProgrammingLanguageGo:
		return "1.26"
	default:
		return "3.12"
	}
}

// executionPhaseFromResult агрегирует метрики полного запуска в доменную фазу исполнения.
func executionPhaseFromResult(result execution.RunResult) *domain.ExecutionPhaseResult {
	return &domain.ExecutionPhaseResult{
		ExitCode:    result.Resources.ExitCode,
		DurationMS:  int64(result.Resources.DurationMS),
		MemoryBytes: result.Resources.MemoryPeakBytes,
	}
}

// verdictFromRunResult сравнивает вывод каждого кейса с ожидаемым и выводит итоговый вердикт.
// Вердикт вычисляется в tasks: sandbox возвращает только вывод и мета-данные.
func verdictFromRunResult(result execution.RunResult, expected []domain.TaskExample) (domain.ExecutionVerdict, []domain.ExecutionTestResult, *domain.ExecutionFailure) {
	if len(result.Cases) == 0 {
		return runLevelVerdict(result)
	}

	tests := make([]domain.ExecutionTestResult, 0, len(result.Cases))
	best := domain.ExecutionVerdictAccepted
	for _, run := range result.Cases {
		verdict := caseVerdict(run, expectedAt(expected, run.Index))
		if verdictPriority(verdict) > verdictPriority(best) {
			best = verdict
		}
		tests = append(tests, domain.ExecutionTestResult{
			TestID:      "open-" + strconv.Itoa(run.Index+1),
			Verdict:     verdict,
			Stdout:      run.Stdout,
			Stderr:      run.Stderr,
			DurationMS:  int64(run.CPUms),
			MemoryBytes: run.MemoryPeakBytes,
		})
	}

	var failure *domain.ExecutionFailure
	if result.Error != nil && best != domain.ExecutionVerdictAccepted {
		failure = &domain.ExecutionFailure{Code: result.Error.Code, Message: result.Error.Message}
	}

	return best, tests, failure
}

// runLevelVerdict обрабатывает случай без кейсов: sandbox вернул run-level ошибку.
func runLevelVerdict(result execution.RunResult) (domain.ExecutionVerdict, []domain.ExecutionTestResult, *domain.ExecutionFailure) {
	var verdict domain.ExecutionVerdict
	switch result.Status {
	case execution.ResultStatusTimeLimit:
		verdict = domain.ExecutionVerdictTimeLimitExceeded
	case execution.ResultStatusMemoryLimit:
		verdict = domain.ExecutionVerdictMemoryLimitExceeded
	case execution.ResultStatusOutputLimit:
		verdict = domain.ExecutionVerdictOutputLimitExceeded
	case execution.ResultStatusRuntimeError:
		verdict = domain.ExecutionVerdictRuntimeError
	case execution.ResultStatusInvalidRequest, execution.ResultStatusUnsupportedLang:
		verdict = domain.ExecutionVerdictCheckerError
	default:
		verdict = domain.ExecutionVerdictInfrastructureError
	}
	var failure *domain.ExecutionFailure
	if result.Error != nil {
		failure = &domain.ExecutionFailure{Code: result.Error.Code, Message: result.Error.Message}
	}

	return verdict, []domain.ExecutionTestResult{}, failure
}

// caseVerdict выводит вердикт одного кейса: лимиты/рантайм из статуса песочницы,
// иначе — сравнение stdout с ожидаемым выводом открытого примера.
func caseVerdict(run execution.CaseRunResult, expectedOutput string) domain.ExecutionVerdict {
	switch run.Status {
	case execution.ResultStatusTimeLimit:
		return domain.ExecutionVerdictTimeLimitExceeded
	case execution.ResultStatusMemoryLimit:
		return domain.ExecutionVerdictMemoryLimitExceeded
	case execution.ResultStatusOutputLimit:
		return domain.ExecutionVerdictOutputLimitExceeded
	case execution.ResultStatusRuntimeError:
		return domain.ExecutionVerdictRuntimeError
	case execution.ResultStatusInternalError:
		// Внутренняя ошибка песочницы на кейсе — это инфраструктурная проблема,
		// а не несовпадение вывода: решение нельзя помечать wrong_answer.
		return domain.ExecutionVerdictInfrastructureError
	}
	if run.ExitCode != nil && *run.ExitCode != 0 {
		return domain.ExecutionVerdictRuntimeError
	}
	if strings.TrimRight(run.Stdout, "\r\n") == strings.TrimRight(expectedOutput, "\r\n") {
		return domain.ExecutionVerdictAccepted
	}

	return domain.ExecutionVerdictWrongAnswer
}

// expectedAt возвращает эталон кейса по индексу (индексы совпадают с порядком открытых примеров).
func expectedAt(expected []domain.TaskExample, index int) string {
	if index >= 0 && index < len(expected) {
		return expected[index].Output
	}
	return ""
}

// verdictPriority задаёт порядок значимости вердиктов: выше — серьёзнее.
func verdictPriority(verdict domain.ExecutionVerdict) int {
	switch verdict {
	// Инфраструктурная ошибка — самый серьёзный вердикт: она отменяет
	// любую ошибку, вызванную поведением пользовательского кода.
	case domain.ExecutionVerdictInfrastructureError:
		return 6
	case domain.ExecutionVerdictOutputLimitExceeded:
		return 5
	case domain.ExecutionVerdictMemoryLimitExceeded:
		return 4
	case domain.ExecutionVerdictTimeLimitExceeded:
		return 3
	case domain.ExecutionVerdictRuntimeError:
		return 2
	case domain.ExecutionVerdictWrongAnswer:
		return 1
	default:
		return 0
	}
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

// openExamples возвращает примеры с ожидаемым выводом в исходном порядке.
func openExamples(examples []domain.TaskExample) []domain.TaskExample {
	open := make([]domain.TaskExample, 0, len(examples))
	for _, example := range examples {
		if example.Output == "" {
			continue
		}
		open = append(open, example)
	}

	return open
}

// exampleInputs возвращает входные кейсы открытых примеров (в том же порядке).
func exampleInputs(open []domain.TaskExample) []string {
	inputs := make([]string, len(open))
	for i, example := range open {
		inputs[i] = example.Input
	}

	return inputs
}

// matchesRunResult защищает решение от результата другого запуска или окружения.
func matchesRunResult(submission domain.CodeSubmission, result execution.RunResult) bool {
	submissionID, err := uuid.Parse(result.SubmissionID)
	if err != nil {
		return false
	}
	attemptID, err := uuid.Parse(result.AttemptID)
	if err != nil {
		return false
	}
	taskID, err := uuid.Parse(result.TaskID)
	if err != nil {
		return false
	}

	return submission.ID == submissionID &&
		submission.ExecutionID == attemptID &&
		submission.TaskID == taskID
}

package execution

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
	"github.com/overmindv/tasks-it/internal/domain"
)

const (
	SchemaVersion              = 1
	RequestEventType           = "code_execution.requested"
	ResultEventType            = "code_execution.completed"
	TestVisibilityOpen         = "open"
	InboxStatusProcessed       = "processed"
	InboxStatusRejected        = "rejected"
	InboxErrorInvalidEvent     = "INVALID_EVENT"
	InboxErrorUnsupportedEvent = "UNSUPPORTED_EVENT"
)

// ResourceLimits описывает обязательные ограничения одного запуска.
type ResourceLimits struct {
	TimeMS      int64 `json:"time_ms"`
	MemoryBytes int64 `json:"memory_bytes"`
}

// SourceFile описывает один текстовый файл пользовательского решения.
type SourceFile struct {
	Name    string `json:"name"`
	Content string `json:"content"`
}

// TestCase описывает тест, передаваемый только доверенному sandbox.
type TestCase struct {
	ID             string `json:"id"`
	Visibility     string `json:"visibility"`
	Input          string `json:"input"`
	ExpectedOutput string `json:"expected_output"`
}

// RequestEvent задаёт версионированный контракт запуска кода.
type RequestEvent struct {
	EventID       uuid.UUID                  `json:"event_id"`
	EventType     string                     `json:"event_type"`
	SchemaVersion int                        `json:"schema_version"`
	OccurredAt    time.Time                  `json:"occurred_at"`
	CorrelationID uuid.UUID                  `json:"correlation_id"`
	SubmissionID  uuid.UUID                  `json:"submission_id"`
	ExecutionID   uuid.UUID                  `json:"execution_id"`
	TaskID        uuid.UUID                  `json:"task_id"`
	TaskVersionID uuid.UUID                  `json:"task_version_id"`
	Language      domain.ProgrammingLanguage `json:"language"`
	Source        SourceFile                 `json:"source"`
	Tests         []TestCase                 `json:"tests"`
	Limits        ResourceLimits             `json:"limits"`
}

// ResultEvent задаёт версионированный финальный ответ sandbox.
type ResultEvent struct {
	EventID       uuid.UUID                    `json:"event_id"`
	EventType     string                       `json:"event_type"`
	SchemaVersion int                          `json:"schema_version"`
	OccurredAt    time.Time                    `json:"occurred_at"`
	CorrelationID uuid.UUID                    `json:"correlation_id"`
	SubmissionID  uuid.UUID                    `json:"submission_id"`
	ExecutionID   uuid.UUID                    `json:"execution_id"`
	TaskID        uuid.UUID                    `json:"task_id"`
	TaskVersionID uuid.UUID                    `json:"task_version_id"`
	Verdict       domain.ExecutionVerdict      `json:"verdict"`
	Compilation   *domain.ExecutionPhaseResult `json:"compilation,omitempty"`
	Execution     *domain.ExecutionPhaseResult `json:"execution,omitempty"`
	Tests         []domain.ExecutionTestResult `json:"tests"`
	Failure       *domain.ExecutionFailure     `json:"failure,omitempty"`
}

// EncodeRequest сериализует request event после проверки обязательных полей.
func EncodeRequest(event RequestEvent) ([]byte, error) {
	if err := ValidateRequest(event); err != nil {
		return nil, fmt.Errorf("validate execution request: %w", err)
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("marshal execution request: %w", err)
	}

	return payload, nil
}

// ValidateRequest проверяет обязательные идентификаторы и данные для sandbox.
func ValidateRequest(event RequestEvent) error {
	if event.EventID == uuid.Nil || event.SubmissionID == uuid.Nil || event.ExecutionID == uuid.Nil || event.CorrelationID == uuid.Nil || event.TaskID == uuid.Nil || event.TaskVersionID == uuid.Nil {
		return errors.New("идентификаторы события, решения, запуска, корреляции, задачи и версии обязательны")
	}
	if event.EventType != RequestEventType || event.SchemaVersion != SchemaVersion || event.OccurredAt.IsZero() {
		return errors.New("тип, версия схемы или время request event некорректны")
	}
	if err := domain.ValidateSourceFile(event.Language, event.Source.Name, event.Source.Content); err != nil {
		return fmt.Errorf("validate source file: %w", err)
	}
	if event.Limits.TimeMS <= 0 || event.Limits.MemoryBytes <= 0 {
		return errors.New("лимиты времени и памяти должны быть положительными")
	}
	if len(event.Tests) == 0 {
		return errors.New("для запуска нужен хотя бы один тест")
	}
	for _, test := range event.Tests {
		if test.ID == "" || test.Visibility != TestVisibilityOpen || test.ExpectedOutput == "" {
			return errors.New("тест должен содержать id, open visibility и expected_output")
		}
	}

	return nil
}

// DecodeResult строго декодирует ровно один result event.
func DecodeResult(payload []byte) (ResultEvent, error) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var event ResultEvent
	if err := decoder.Decode(&event); err != nil {
		return ResultEvent{}, fmt.Errorf("decode execution result: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return ResultEvent{}, errors.New("execution result должен содержать один JSON-объект")
	}
	if err := ValidateResult(event); err != nil {
		return ResultEvent{}, fmt.Errorf("validate execution result: %w", err)
	}

	return event, nil
}

// ValidateResult проверяет envelope, verdict и безопасные границы вывода sandbox.
func ValidateResult(event ResultEvent) error {
	if event.EventID == uuid.Nil || event.SubmissionID == uuid.Nil || event.ExecutionID == uuid.Nil || event.CorrelationID == uuid.Nil || event.TaskID == uuid.Nil || event.TaskVersionID == uuid.Nil {
		return errors.New("идентификаторы result event обязательны")
	}
	if event.EventType != ResultEventType {
		return fmt.Errorf("неподдерживаемый event_type %q", event.EventType)
	}
	if event.SchemaVersion != SchemaVersion {
		return fmt.Errorf("неподдерживаемая schema_version %d", event.SchemaVersion)
	}
	if event.OccurredAt.IsZero() {
		return errors.New("occurred_at обязателен")
	}
	if !domain.IsFinalExecutionVerdict(event.Verdict) {
		return fmt.Errorf("неподдерживаемый verdict %q", event.Verdict)
	}
	if event.Tests == nil || len(event.Tests) > domain.MaxExecutionTestResults {
		return fmt.Errorf("tests должен быть массивом не более чем из %d результатов", domain.MaxExecutionTestResults)
	}
	if err := validatePhase(event.Compilation); err != nil {
		return fmt.Errorf("validate compilation: %w", err)
	}
	if err := validatePhase(event.Execution); err != nil {
		return fmt.Errorf("validate execution: %w", err)
	}
	for _, test := range event.Tests {
		if test.TestID == "" || !domain.IsFinalExecutionVerdict(test.Verdict) {
			return errors.New("результат теста содержит пустой test_id или неизвестный verdict")
		}
		if test.DurationMS < 0 || test.MemoryBytes < 0 || len(test.Stdout) > domain.MaxExecutionOutputSize || len(test.Stderr) > domain.MaxExecutionOutputSize {
			return errors.New("результат теста выходит за допустимые границы")
		}
	}
	if event.Failure != nil && (!isTechnicalCode(event.Failure.Code) || len(event.Failure.Code) > 100 || len(event.Failure.Message) > 2000) {
		return errors.New("failure должен содержать code и ограниченный message")
	}

	return nil
}

// isTechnicalCode проверяет машинный код в формате SNAKE_CASE.
func isTechnicalCode(value string) bool {
	if value == "" {
		return false
	}
	for _, character := range value {
		if character != '_' && (character < 'A' || character > 'Z') && (character < '0' || character > '9') {
			return false
		}
	}

	return true
}

// validatePhase проверяет метрики и размер вывода одной фазы.
func validatePhase(phase *domain.ExecutionPhaseResult) error {
	if phase == nil {
		return nil
	}
	if phase.DurationMS < 0 || phase.MemoryBytes < 0 {
		return errors.New("duration_ms и memory_bytes не могут быть отрицательными")
	}
	if len(phase.Stdout) > domain.MaxExecutionOutputSize || len(phase.Stderr) > domain.MaxExecutionOutputSize {
		return fmt.Errorf("stdout и stderr не должны превышать %d байт", domain.MaxExecutionOutputSize)
	}

	return nil
}

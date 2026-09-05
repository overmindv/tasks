package domain

import (
	"fmt"
	"path"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
)

const (
	MaxSourceFileSize       = 256 * 1024
	MaxSourceFileNameLength = 255
	MaxExecutionOutputSize  = 64 * 1024
	MaxExecutionTestResults = MaxExamples
)

// ProgrammingLanguage описывает язык загружаемого решения.
type ProgrammingLanguage string

const (
	ProgrammingLanguagePython ProgrammingLanguage = "python"
	ProgrammingLanguageGo     ProgrammingLanguage = "go"
)

// CodeSubmissionStatus описывает асинхронный lifecycle решения программной задачи.
type CodeSubmissionStatus string

const (
	CodeSubmissionStatusQueued    CodeSubmissionStatus = "queued"
	CodeSubmissionStatusCompleted CodeSubmissionStatus = "completed"
)

// ExecutionVerdict описывает итог, полученный от sandbox.
type ExecutionVerdict string

const (
	ExecutionVerdictAccepted            ExecutionVerdict = "accepted"
	ExecutionVerdictWrongAnswer         ExecutionVerdict = "wrong_answer"
	ExecutionVerdictCompilationError    ExecutionVerdict = "compilation_error"
	ExecutionVerdictRuntimeError        ExecutionVerdict = "runtime_error"
	ExecutionVerdictTimeLimitExceeded   ExecutionVerdict = "time_limit_exceeded"
	ExecutionVerdictMemoryLimitExceeded ExecutionVerdict = "memory_limit_exceeded"
	ExecutionVerdictOutputLimitExceeded ExecutionVerdict = "output_limit_exceeded"
	ExecutionVerdictCheckerError        ExecutionVerdict = "checker_error"
	ExecutionVerdictInfrastructureError ExecutionVerdict = "infrastructure_error"
	ExecutionVerdictCancelled           ExecutionVerdict = "cancelled"
)

// ExecutionPhaseResult хранит безопасный результат компиляции или запуска.
type ExecutionPhaseResult struct {
	ExitCode    *int   `json:"exit_code,omitempty"`
	Stdout      string `json:"stdout"`
	Stderr      string `json:"stderr"`
	DurationMS  int64  `json:"duration_ms"`
	MemoryBytes int64  `json:"memory_bytes"`
}

// ExecutionTestResult хранит результат одного теста без эталонного ответа.
type ExecutionTestResult struct {
	TestID      string           `json:"test_id"`
	Verdict     ExecutionVerdict `json:"verdict"`
	Stdout      string           `json:"stdout"`
	Stderr      string           `json:"stderr"`
	DurationMS  int64            `json:"duration_ms"`
	MemoryBytes int64            `json:"memory_bytes"`
}

// ExecutionFailure хранит машинный код и безопасное описание ошибки runner'а.
type ExecutionFailure struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// CodeSubmission хранит файл решения и последний подтверждённый результат sandbox.
type CodeSubmission struct {
	ID                uuid.UUID
	UserID            uuid.UUID
	TaskID            uuid.UUID
	TaskVersionID     uuid.UUID
	TaskVersionNumber int
	ExecutionID       uuid.UUID
	CorrelationID     uuid.UUID
	IdempotencyKey    uuid.UUID
	RequestHash       string
	Language          ProgrammingLanguage
	SourceFileName    string
	SourceCode        string
	Status            CodeSubmissionStatus
	Verdict           *ExecutionVerdict
	Compilation       *ExecutionPhaseResult
	Execution         *ExecutionPhaseResult
	Tests             []ExecutionTestResult
	Failure           *ExecutionFailure
	CreatedAt         time.Time
	UpdatedAt         time.Time
	CompletedAt       *time.Time
}

// CodeSubmissionFilter задаёт фильтр истории программных решений пользователя.
type CodeSubmissionFilter struct {
	UserID uuid.UUID
	TaskID *uuid.UUID
	Limit  int
	Offset int
}

// OutboxMessage хранит подготовленное Kafka-событие до подтверждённой публикации.
type OutboxMessage struct {
	ID           uuid.UUID
	AggregateID  uuid.UUID
	Topic        string
	MessageKey   string
	Payload      []byte
	Attempts     int
	ClaimToken   *uuid.UUID
	ClaimedUntil *time.Time
	AvailableAt  time.Time
	CreatedAt    time.Time
}

// ExecutionInboxRecord хранит идентификаторы обработанного или отклонённого Kafka-сообщения.
type ExecutionInboxRecord struct {
	ID            uuid.UUID
	EventID       *uuid.UUID
	Topic         string
	Partition     int32
	Offset        int64
	PayloadSHA256 string
	Status        string
	ErrorCode     string
}

// NormalizeSourceFile очищает имя файла, не сохраняя пользовательский путь.
func NormalizeSourceFile(fileName, sourceCode string) (string, string) {
	fileName = strings.ReplaceAll(fileName, "\\", "/")
	fileName = path.Base(strings.TrimSpace(fileName))

	return fileName, sourceCode
}

// DefaultSourceFileName возвращает каноническое имя файла решения по языку
// (вариант «код в консоли», когда файл не загружается).
func DefaultSourceFileName(language ProgrammingLanguage) (string, error) {
	switch language {
	case ProgrammingLanguagePython:
		return "solution.py", nil
	case ProgrammingLanguageGo:
		return "main.go", nil
	default:
		return "", fmt.Errorf("неподдерживаемый язык %q", language)
	}
}

// ValidateSourceFile проверяет allowlist языка, расширение, размер и текстовый формат файла.
func ValidateSourceFile(language ProgrammingLanguage, fileName, sourceCode string) error {
	if language != ProgrammingLanguagePython && language != ProgrammingLanguageGo {
		return fmt.Errorf("неподдерживаемый язык %q", language)
	}
	if fileName == "" || fileName == "." || len([]rune(fileName)) > MaxSourceFileNameLength {
		return fmt.Errorf("имя файла должно содержать от 1 до %d символов", MaxSourceFileNameLength)
	}
	expectedExtension := ".py"
	if language == ProgrammingLanguageGo {
		expectedExtension = ".go"
	}
	if !strings.EqualFold(path.Ext(fileName), expectedExtension) {
		return fmt.Errorf("для языка %s требуется файл %s", language, expectedExtension)
	}
	if sourceCode == "" {
		return fmt.Errorf("файл решения пуст")
	}
	if len(sourceCode) > MaxSourceFileSize {
		return fmt.Errorf("размер файла не должен превышать %d байт", MaxSourceFileSize)
	}
	if !utf8.ValidString(sourceCode) || strings.IndexByte(sourceCode, 0) >= 0 {
		return fmt.Errorf("файл решения должен быть текстом UTF-8 без NUL-байтов")
	}

	return nil
}

// IsFinalExecutionVerdict проверяет allowlist финальных verdict'ов sandbox.
func IsFinalExecutionVerdict(verdict ExecutionVerdict) bool {
	switch verdict {
	case ExecutionVerdictAccepted,
		ExecutionVerdictWrongAnswer,
		ExecutionVerdictCompilationError,
		ExecutionVerdictRuntimeError,
		ExecutionVerdictTimeLimitExceeded,
		ExecutionVerdictMemoryLimitExceeded,
		ExecutionVerdictOutputLimitExceeded,
		ExecutionVerdictCheckerError,
		ExecutionVerdictInfrastructureError,
		ExecutionVerdictCancelled:
		return true
	default:
		return false
	}
}

package execution

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
)

// Константы контракта исполнения кода между tasks и sandbox
const (
	RequestSchemaVersion = "sandbox.run.request.v1"
	ResultSchemaVersion  = "sandbox.run.result.v1"
	ExecutionMode        = "execution"

	// CaseStatusOK задаёт статус кейса, который отработал в пределах лимитов.
	CaseStatusOK = "ok"

	// Статусы результата, возвращаемые sandbox
	ResultStatusOK              = "ok"
	ResultStatusTimeLimit       = "tle"
	ResultStatusMemoryLimit     = "mle"
	ResultStatusOutputLimit     = "ole"
	ResultStatusRuntimeError    = "runtime_error"
	ResultStatusInternalError   = "internal_error"
	ResultStatusInvalidRequest  = "invalid_request"
	ResultStatusUnsupportedLang = "unsupported_language"

	InboxStatusProcessed       = "processed"
	InboxStatusRejected        = "rejected"
	InboxErrorInvalidEvent     = "INVALID_EVENT"
	InboxErrorUnsupportedEvent = "UNSUPPORTED_EVENT"

	// MaxExecutionCases задаёт верхнюю границу числа входных кейсов на запуск.
	MaxExecutionCases = 20
	// MaxExecutionOutputBytes задаёт безопасный размер вывода на один кейс.
	MaxExecutionOutputBytes = 64 * 1024
)

// Language описывает язык и версию решения.
type Language struct {
	ID      string `json:"id"`
	Version string `json:"version"`
}

// SourceFile описывает один файл решения, передаваемый песочнице.
type SourceFile struct {
	Path           string `json:"path"`
	ContentB64     string `json:"content_b64"`
	ChecksumSHA256 string `json:"checksum_sha256,omitempty"`
}

// Code описывает точку входа и файлы пользовательского решения.
type Code struct {
	Entrypoint string       `json:"entrypoint,omitempty"`
	Files      []SourceFile `json:"files"`
}

// ExecutionSpec задаёт execution-режим: исполнение кода по списку входных данных.
type ExecutionSpec struct {
	Mode   string   `json:"mode"`
	Inputs []string `json:"inputs"`
}

// Limits описывает обязательные ограничения одного запуска.
type Limits struct {
	CPUms          int `json:"cpu_ms"`
	Wallms         int `json:"wall_ms"`
	MemoryMB       int `json:"memory_mb"`
	PIDs           int `json:"pids"`
	StdoutBytes    int `json:"stdout_bytes"`
	StderrBytes    int `json:"stderr_bytes"`
	WorkspaceBytes int `json:"workspace_bytes"`
}

// RunRequest описывает запрос на исполнение кода, который tasks публикует в Kafka.
type RunRequest struct {
	SchemaVersion string         `json:"schema_version"`
	SubmissionID  string         `json:"submission_id"`
	AttemptID     string         `json:"attempt_id"`
	UserID        string         `json:"user_id"`
	TaskID        string         `json:"task_id"`
	Language      Language       `json:"language"`
	Code          Code           `json:"code"`
	Execution     *ExecutionSpec `json:"execution,omitempty"`
	Limits        Limits         `json:"limits"`
	Metadata      map[string]any `json:"metadata,omitempty"`
	TraceID       string         `json:"trace_id"`
	CreatedAt     time.Time      `json:"created_at"`
}

// CaseRunResult описывает результат исполнения одного входного кейса.
type CaseRunResult struct {
	Index           int     `json:"index"`
	Stdout          string  `json:"stdout"`
	Stderr          string  `json:"stderr"`
	Truncated       bool    `json:"truncated"`
	ExitCode        *int    `json:"exit_code"`
	Signal          *string `json:"signal"`
	CPUms           int     `json:"cpu_ms,omitempty"`
	MemoryPeakBytes int64   `json:"memory_peak_bytes,omitempty"`
	DurationMS      int     `json:"duration_ms,omitempty"`
	Status          string  `json:"status"`
}

// ResultSummary содержит агрегированные счётчики полного запуска.
// Поля tests_* и first_failed_test объявлены для совместимости со строгим декодом
// sandbox-контракта (песочница всегда шлёт их в pytest-режиме).
type ResultSummary struct {
	CasesTotal      int    `json:"cases_total"`
	CasesWithError  int    `json:"cases_with_error"`
	TestsTotal      int    `json:"tests_total"`
	TestsPassed     int    `json:"tests_passed"`
	TestsFailed     int    `json:"tests_failed"`
	FirstFailedTest string `json:"first_failed_test,omitempty"`
}

// ResultResources содержит агрегированные метрики полного запуска.
type ResultResources struct {
	CPUms           int     `json:"cpu_ms"`
	MemoryPeakBytes int64   `json:"memory_peak_bytes"`
	ExitCode        *int    `json:"exit_code"`
	Signal          *string `json:"signal"`
	DurationMS      int     `json:"duration_ms,omitempty"`
}

// RunError содержит машинный код и безопасное описание ошибки песочницы.
type RunError struct {
	Code      string         `json:"code"`
	Message   string         `json:"message"`
	Retryable bool           `json:"retryable"`
	Details   map[string]any `json:"details,omitempty"`
}

// RunResult описывает финальный ответ песочницы на запрос исполнения кода.
type RunResult struct {
	SchemaVersion string          `json:"schema_version"`
	SubmissionID  string          `json:"submission_id"`
	AttemptID     string          `json:"attempt_id"`
	UserID        string          `json:"user_id,omitempty"`
	TaskID        string          `json:"task_id,omitempty"`
	Language      Language        `json:"language,omitempty"`
	Status        string          `json:"status"`
	Summary       ResultSummary   `json:"summary"`
	Resources     ResultResources `json:"resources"`
	// Timing, Logs и Tests sandbox всегда включает в result даже в execution-режиме.
	// tasks их не использует, но объявляет через RawMessage, чтобы строгий декод
	// (DisallowUnknownFields) не отклонял события песочницы как «невалидные».
	Timing    json.RawMessage `json:"timing"`
	Logs      json.RawMessage `json:"logs"`
	Tests     json.RawMessage `json:"tests,omitempty"`
	Cases     []CaseRunResult `json:"cases,omitempty"`
	Error     *RunError       `json:"error"`
	TraceID   string          `json:"trace_id,omitempty"`
	WorkerID  string          `json:"worker_id,omitempty"`
	CreatedAt time.Time       `json:"created_at"`
}

// EncodeRunRequest сериализует request event после проверки обязательных полей.
func EncodeRunRequest(request RunRequest) ([]byte, error) {
	if err := ValidateRunRequest(request); err != nil {
		return nil, fmt.Errorf("validate execution request: %w", err)
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("marshal execution request: %w", err)
	}

	return payload, nil
}

// ValidateRunRequest проверяет обязательные идентификаторы, файл, входы и лимиты.
func ValidateRunRequest(request RunRequest) error {
	if request.SchemaVersion != RequestSchemaVersion {
		return fmt.Errorf("неподдерживаемая schema_version %q", request.SchemaVersion)
	}
	if request.SubmissionID == "" || request.AttemptID == "" || request.UserID == "" || request.TaskID == "" {
		return errors.New("submission_id, attempt_id, user_id и task_id обязательны")
	}
	if request.Language.ID == "" || request.Language.Version == "" {
		return errors.New("language.id и language.version обязательны")
	}
	if len(request.Code.Files) == 0 {
		return errors.New("для запуска нужен хотя бы один файл решения")
	}
	for _, file := range request.Code.Files {
		if file.Path == "" || file.ContentB64 == "" {
			return errors.New("каждый файл должен содержать path и content_b64")
		}
	}
	if request.Execution == nil || request.Execution.Mode != ExecutionMode {
		return errors.New("execution.mode должен быть 'execution'")
	}
	if len(request.Execution.Inputs) == 0 {
		return errors.New("для запуска нужен хотя бы один входной кейс")
	}
	if request.Limits.CPUms <= 0 || request.Limits.Wallms <= 0 || request.Limits.MemoryMB <= 0 {
		return errors.New("лимиты времени и памяти должны быть положительными")
	}
	if request.Limits.Wallms < request.Limits.CPUms {
		return errors.New("wall_ms должен быть не меньше cpu_ms")
	}
	if request.CreatedAt.IsZero() {
		return errors.New("created_at обязателен")
	}

	return nil
}

// DecodeRunResult строго декодирует ровно один result event песочницы.
func DecodeRunResult(payload []byte) (RunResult, error) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var result RunResult
	if err := decoder.Decode(&result); err != nil {
		return RunResult{}, fmt.Errorf("decode execution result: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return RunResult{}, errors.New("execution result должен содержать один JSON-объект")
	}
	if err := ValidateRunResult(result); err != nil {
		return RunResult{}, fmt.Errorf("validate execution result: %w", err)
	}

	return result, nil
}

// ValidateRunResult проверяет envelope, статус и безопасные границы вывода песочницы.
func ValidateRunResult(result RunResult) error {
	if result.SchemaVersion != ResultSchemaVersion {
		return fmt.Errorf("неподдерживаемая schema_version %q", result.SchemaVersion)
	}
	if result.SubmissionID == "" || result.AttemptID == "" {
		return errors.New("submission_id и attempt_id обязательны")
	}
	if result.Status == "" {
		return errors.New("status обязателен")
	}
	if result.CreatedAt.IsZero() {
		return errors.New("created_at обязателен")
	}
	if len(result.Cases) > MaxExecutionCases {
		return fmt.Errorf("число кейсов не должно превышать %d", MaxExecutionCases)
	}
	for _, run := range result.Cases {
		if run.Index < 0 || run.Index >= len(result.Cases) {
			return errors.New("индекс кейса выходит за границы массива")
		}
		if run.Status == "" {
			return errors.New("каждый кейс должен содержать status")
		}
		if len(run.Stdout) > MaxExecutionOutputBytes || len(run.Stderr) > MaxExecutionOutputBytes {
			return errors.New("вывод кейса превышает допустимый размер")
		}
	}
	if result.Error != nil && (result.Error.Code == "" || len(result.Error.Message) > 2000) {
		return errors.New("error должен содержать code и ограниченный message")
	}

	return nil
}

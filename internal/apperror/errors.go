package apperror

import "fmt"

const (
	AuthenticationRequired    = "AUTHENTICATION_REQUIRED"
	PermissionDenied          = "PERMISSION_DENIED"
	ValidationError           = "VALIDATION_ERROR"
	TaskNotFound              = "TASK_NOT_FOUND"
	TaskVersionNotFound       = "TASK_VERSION_NOT_FOUND"
	TaskNotAvailable          = "TASK_NOT_AVAILABLE"
	SubmissionNotFound        = "SUBMISSION_NOT_FOUND"
	CodeSubmissionNotFound    = "CODE_SUBMISSION_NOT_FOUND"
	TaskNotExecutable         = "TASK_NOT_EXECUTABLE"
	InvalidSourceFile         = "INVALID_SOURCE_FILE"
	ExecutionResultMismatch   = "EXECUTION_RESULT_MISMATCH"
	InvalidStatusTransition   = "INVALID_STATUS_TRANSITION"
	InvalidAnswer             = "INVALID_ANSWER"
	IdempotencyKeyConflict    = "IDEMPOTENCY_KEY_CONFLICT"
	CandidateNotFound         = "CANDIDATE_NOT_FOUND"
	CandidateRevisionConflict = "CANDIDATE_REVISION_CONFLICT"
	CandidateNotPending       = "CANDIDATE_NOT_PENDING"
	TaskTypeNotSubmittable    = "TASK_TYPE_NOT_SUBMITTABLE"
	InternalError             = "INTERNAL_ERROR"
)

// Error описывает безопасную публичную ошибку HTTP API.
type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Status  int    `json:"-"`
}

// Error возвращает строковое представление публичной ошибки.
func (e *Error) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// New создаёт публичную ошибку с машинным кодом и HTTP-статусом.
func New(code, message string, status int) *Error {
	return &Error{
		Code:    code,
		Message: message,
		Status:  status,
	}
}

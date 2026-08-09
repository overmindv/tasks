package domain

import (
	"time"

	"github.com/google/uuid"
)

// Submission хранит неизменяемый результат одной пользовательской отправки.
type Submission struct {
	ID                  uuid.UUID
	UserID              uuid.UUID
	TaskID              uuid.UUID
	TaskVersionID       uuid.UUID
	TaskVersionNumber   int
	IdempotencyKey      uuid.UUID
	RequestHash         string
	Verdict             Verdict
	SelectedOptionIDs   []uuid.UUID
	CorrectOptionIDs    []uuid.UUID
	LatestTaskVersionID uuid.UUID
	LatestVersionNumber int
	TaskUpdated         bool
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

// SubmissionFilter задаёт фильтр истории решений пользователя.
type SubmissionFilter struct {
	UserID uuid.UUID
	TaskID *uuid.UUID
	Limit  int
	Offset int
}

// IsCorrect возвращает true для принятого ответа.
func (s Submission) IsCorrect() bool {
	return s.Verdict == VerdictAccepted
}

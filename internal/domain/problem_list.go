package domain

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	MaxProblemListNameLength   = 200
	MaxProblemListURLLength    = 1000
	MaxProblemListSourceLength = 100
)

// ProblemList — именованный набор задач из внешнего problem-list.
// Владелец сущности — tasks; создаётся идемпотентно по source_url при импорте.
type ProblemList struct {
	ID        uuid.UUID
	Name      string
	SourceID  string
	SourceURL string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// ProblemListImport описывает service-to-service payload сбора списка от task-hunter.
type ProblemListImport struct {
	Name      string
	SourceID  string
	SourceURL string
}

// ProblemListSummary — список с числом опубликованных задач-членов.
type ProblemListSummary struct {
	ProblemList
	MemberCount int
}

// ProblemListMember — опубликованная задача в составе списка.
type ProblemListMember struct {
	TaskID     uuid.UUID
	Title      string
	Difficulty Difficulty
	Status     TaskStatus
}

// ProblemListDetail — список вместе с членами и числом ожидающих кандидатов.
type ProblemListDetail struct {
	ProblemList
	MemberCount       int
	PendingCandidates int
	Members           []ProblemListMember
}

// NormalizeProblemListImport очищает строки payload импорта списка.
func NormalizeProblemListImport(input ProblemListImport) ProblemListImport {
	input.Name = strings.TrimSpace(input.Name)
	input.SourceID = strings.TrimSpace(input.SourceID)
	input.SourceURL = strings.TrimSpace(input.SourceURL)

	return input
}

// ValidateProblemListImport проверяет импортируемый payload списка.
func ValidateProblemListImport(input ProblemListImport) error {
	if input.Name == "" || len([]rune(input.Name)) > MaxProblemListNameLength {
		return fmt.Errorf("имя списка должно содержать от 1 до %d символов", MaxProblemListNameLength)
	}
	if input.SourceID == "" || len([]rune(input.SourceID)) > MaxProblemListSourceLength {
		return fmt.Errorf("source_id списка обязателен и не должен превышать %d символов", MaxProblemListSourceLength)
	}
	if input.SourceURL == "" || len([]rune(input.SourceURL)) > MaxProblemListURLLength {
		return fmt.Errorf("source_url списка обязателен и не должен превышать %d символов", MaxProblemListURLLength)
	}

	return nil
}

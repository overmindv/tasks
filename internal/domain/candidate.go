package domain

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// CandidateStatus описывает lifecycle собранной задачи до публикации.
type CandidateStatus string

const (
	CandidateStatusPending  CandidateStatus = "pending"
	CandidateStatusApproved CandidateStatus = "approved"
	CandidateStatusRejected CandidateStatus = "rejected"
)

// TaskCandidate хранит нормализованную задачу и неизменяемую атрибуцию источника.
type TaskCandidate struct {
	ID                uuid.UUID
	Status            CandidateStatus
	Revision          int
	ExternalID        string
	SourceID          string
	SourceName        string
	SourceURL         string
	SourceHash        string
	SourcePublishedAt *time.Time
	RetrievedAt       time.Time
	CollectionJobID   uuid.UUID
	TopicID           *uuid.UUID
	Title             string
	Statement         string
	Difficulty        Difficulty
	Tags              []string
	Examples          []TaskExample
	Constraints       []string
	ApprovedTaskID    *uuid.UUID
	ReviewedBy        *uuid.UUID
	ReviewedAt        *time.Time
	RejectionReason   string
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// CandidateImport описывает service-to-service payload от task-hunter.
type CandidateImport struct {
	ExternalID        string
	SourceID          string
	SourceName        string
	SourceURL         string
	SourceHash        string
	SourcePublishedAt *time.Time
	RetrievedAt       time.Time
	CollectionJobID   uuid.UUID
	Title             string
	Statement         string
	Difficulty        Difficulty
	Tags              []string
	Examples          []TaskExample
	Constraints       []string
}

// CandidateReview содержит редактируемые администратором поля кандидата.
type CandidateReview struct {
	ExpectedRevision int
	TopicID          *uuid.UUID
	Title            string
	Statement        string
	Difficulty       Difficulty
	Tags             []string
	Examples         []TaskExample
	Constraints      []string
}

// CandidateFilter задаёт административные фильтры очереди.
type CandidateFilter struct {
	Status     *CandidateStatus
	SourceID   string
	Difficulty *Difficulty
	Limit      int
	Offset     int
}

// NormalizeCandidateImport очищает импортированные данные до проверки.
func NormalizeCandidateImport(input CandidateImport) CandidateImport {
	input.ExternalID = strings.TrimSpace(input.ExternalID)
	input.SourceID = strings.TrimSpace(input.SourceID)
	input.SourceName = strings.TrimSpace(input.SourceName)
	input.SourceURL = strings.TrimSpace(input.SourceURL)
	input.SourceHash = strings.TrimSpace(input.SourceHash)
	normalized := NormalizeTaskInput(TaskInput{
		Title:       input.Title,
		Statement:   input.Statement,
		TaskType:    TaskTypeProgramming,
		Difficulty:  input.Difficulty,
		Tags:        input.Tags,
		Examples:    input.Examples,
		Constraints: input.Constraints,
	})
	input.Title = normalized.Title
	input.Statement = normalized.Statement
	input.Difficulty = normalized.Difficulty
	input.Tags = normalized.Tags
	input.Examples = normalized.Examples
	input.Constraints = normalized.Constraints

	return input
}

// ValidateCandidateImport проверяет атрибуцию и содержимое кандидата.
func ValidateCandidateImport(input CandidateImport) error {
	if input.ExternalID == "" || input.SourceID == "" || input.SourceURL == "" || input.SourceHash == "" {
		return fmt.Errorf("external_id, source_id, source_url и source_hash обязательны")
	}
	if input.CollectionJobID == uuid.Nil || input.RetrievedAt.IsZero() {
		return fmt.Errorf("collection_job_id и retrieved_at обязательны")
	}

	return ValidateTaskInput(TaskInput{
		Title:       input.Title,
		Statement:   input.Statement,
		TaskType:    TaskTypeProgramming,
		Difficulty:  input.Difficulty,
		Tags:        input.Tags,
		Examples:    input.Examples,
		Constraints: input.Constraints,
	})
}

// NormalizeCandidateReview очищает редактируемые поля кандидата.
func NormalizeCandidateReview(input CandidateReview) CandidateReview {
	normalized := NormalizeTaskInput(TaskInput{
		TopicID:     input.TopicID,
		Title:       input.Title,
		Statement:   input.Statement,
		TaskType:    TaskTypeProgramming,
		Difficulty:  input.Difficulty,
		Tags:        input.Tags,
		Examples:    input.Examples,
		Constraints: input.Constraints,
	})
	input.TopicID = normalized.TopicID
	input.Title = normalized.Title
	input.Statement = normalized.Statement
	input.Difficulty = normalized.Difficulty
	input.Tags = normalized.Tags
	input.Examples = normalized.Examples
	input.Constraints = normalized.Constraints

	return input
}

// ValidateCandidateReview проверяет optimistic revision и содержимое после модерации.
func ValidateCandidateReview(input CandidateReview) error {
	if input.ExpectedRevision <= 0 {
		return fmt.Errorf("expected_revision должен быть положительным")
	}

	return ValidateTaskInput(TaskInput{
		TopicID:     input.TopicID,
		Title:       input.Title,
		Statement:   input.Statement,
		TaskType:    TaskTypeProgramming,
		Difficulty:  input.Difficulty,
		Tags:        input.Tags,
		Examples:    input.Examples,
		Constraints: input.Constraints,
	})
}

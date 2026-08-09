package domain

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/samber/lo"
)

const (
	MaxOptions         = 20
	MaxTitleLength     = 200
	MaxStatementLength = 50000
	MaxOptionLength    = 2000
)

// TaskStatus описывает lifecycle тестовой задачи.
type TaskStatus string

const (
	TaskStatusDraft     TaskStatus = "draft"
	TaskStatusPublished TaskStatus = "published"
	TaskStatusArchived  TaskStatus = "archived"
)

// TaskType описывает поддерживаемый формат ответа.
type TaskType string

const (
	TaskTypeSingleChoice   TaskType = "single_choice"
	TaskTypeMultipleChoice TaskType = "multiple_choice"
)

// Difficulty описывает сложность теста.
type Difficulty string

const (
	DifficultyEasy   Difficulty = "easy"
	DifficultyMedium Difficulty = "medium"
	DifficultyHard   Difficulty = "hard"
)

// Verdict описывает итог проверки ответа.
type Verdict string

const (
	VerdictAccepted    Verdict = "accepted"
	VerdictWrongAnswer Verdict = "wrong_answer"
)

// Task хранит lifecycle и аудит агрегата задачи.
type Task struct {
	ID               uuid.UUID
	CurrentVersionID *uuid.UUID
	Status           TaskStatus
	CreatedBy        uuid.UUID
	UpdatedBy        uuid.UUID
	CreatedAt        time.Time
	UpdatedAt        time.Time
	DeletedAt        *time.Time
}

// TaskVersion хранит неизменяемое содержимое конкретной версии теста.
type TaskVersion struct {
	ID            uuid.UUID
	TaskID        uuid.UUID
	VersionNumber int
	TopicID       *uuid.UUID
	Title         string
	Statement     string
	TaskType      TaskType
	Difficulty    Difficulty
	CreatedBy     uuid.UUID
	CreatedAt     time.Time
	UpdatedAt     time.Time
	Options       []TaskOption
}

// TaskOption хранит один вариант ответа конкретной версии.
type TaskOption struct {
	ID            uuid.UUID
	TaskVersionID uuid.UUID
	Text          string
	IsCorrect     bool
	Position      int
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// TaskDetail объединяет lifecycle задачи с её текущей версией.
type TaskDetail struct {
	Task    Task
	Version TaskVersion
}

// TaskInput описывает полную запись новой версии теста.
type TaskInput struct {
	TopicID    *uuid.UUID
	Title      string
	Statement  string
	TaskType   TaskType
	Difficulty Difficulty
	Options    []OptionInput
}

// OptionInput описывает вариант ответа до сохранения.
type OptionInput struct {
	Text      string
	IsCorrect bool
}

// TaskFilter задаёт явные фильтры и pagination списка задач.
type TaskFilter struct {
	Status     *TaskStatus
	TaskType   *TaskType
	Difficulty *Difficulty
	TopicID    *uuid.UUID
	Limit      int
	Offset     int
}

// NormalizeTaskInput очищает пользовательские строки и задаёт сложность по умолчанию.
func NormalizeTaskInput(input TaskInput) TaskInput {
	input.Title = strings.TrimSpace(input.Title)
	input.Statement = strings.TrimSpace(input.Statement)
	if input.Difficulty == "" {
		input.Difficulty = DifficultyEasy
	}
	input.Options = lo.Map(input.Options, func(option OptionInput, _ int) OptionInput {
		option.Text = strings.TrimSpace(option.Text)

		return option
	})

	return input
}

// ValidateTaskInput проверяет инварианты поддерживаемых тестов.
func ValidateTaskInput(input TaskInput) error {
	if input.Title == "" || len([]rune(input.Title)) > MaxTitleLength {
		return fmt.Errorf("название должно содержать от 1 до %d символов", MaxTitleLength)
	}
	if input.Statement == "" || len([]rune(input.Statement)) > MaxStatementLength {
		return fmt.Errorf("условие должно содержать от 1 до %d символов", MaxStatementLength)
	}
	if input.TaskType != TaskTypeSingleChoice && input.TaskType != TaskTypeMultipleChoice {
		return fmt.Errorf("неподдерживаемый task_type %q", input.TaskType)
	}
	if input.Difficulty != DifficultyEasy && input.Difficulty != DifficultyMedium && input.Difficulty != DifficultyHard {
		return fmt.Errorf("неподдерживаемая difficulty %q", input.Difficulty)
	}
	if len(input.Options) < 2 || len(input.Options) > MaxOptions {
		return fmt.Errorf("количество вариантов должно быть от 2 до %d", MaxOptions)
	}
	if err := validateOptions(input.Options); err != nil {
		return fmt.Errorf("проверить варианты ответа: %w", err)
	}

	return validateCorrectOptions(input.TaskType, input.Options)
}

// CanTransition проверяет разрешённый переход lifecycle задачи.
func CanTransition(from, to TaskStatus) bool {
	return from == TaskStatusDraft && to == TaskStatusPublished ||
		from == TaskStatusPublished && to == TaskStatusArchived ||
		from == TaskStatusArchived && to == TaskStatusDraft
}

// NormalizePagination задаёт безопасные значения limit и offset.
func NormalizePagination(limit, offset int) (int, int) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}

	return limit, offset
}

// validateOptions проверяет тексты вариантов и их уникальность.
func validateOptions(options []OptionInput) error {
	names := make([]string, 0, len(options))
	for _, option := range options {
		if option.Text == "" || len([]rune(option.Text)) > MaxOptionLength {
			return fmt.Errorf("текст варианта должен содержать от 1 до %d символов", MaxOptionLength)
		}
		names = append(names, strings.ToLower(option.Text))
	}
	if len(lo.Uniq(names)) != len(names) {
		return fmt.Errorf("варианты ответа не должны повторяться")
	}

	return nil
}

// validateCorrectOptions проверяет число правильных вариантов для типа теста.
func validateCorrectOptions(taskType TaskType, options []OptionInput) error {
	correctCount := lo.CountBy(options, func(option OptionInput) bool { return option.IsCorrect })
	if taskType == TaskTypeSingleChoice && correctCount != 1 {
		return fmt.Errorf("single_choice должен содержать ровно один правильный вариант")
	}
	if taskType == TaskTypeMultipleChoice && (correctCount == 0 || correctCount == len(options)) {
		return fmt.Errorf("multiple_choice должен содержать правильные и неправильные варианты")
	}

	return nil
}

package httpapi

import (
	"time"

	"github.com/google/uuid"
	"github.com/overmindv/tasks-it/internal/domain"
	"github.com/overmindv/tasks-it/internal/usecase"
	"github.com/samber/lo"
)

type taskInput struct {
	TopicID     *string        `json:"topic_id"`
	Title       string         `json:"title"`
	Statement   string         `json:"statement"`
	TaskType    string         `json:"task_type"`
	Difficulty  string         `json:"difficulty"`
	Options     []optionInput  `json:"options"`
	Tags        []string       `json:"tags"`
	Examples    []exampleInput `json:"examples"`
	Constraints []string       `json:"constraints"`
	Source      *sourceInput   `json:"source"`
}

type exampleInput struct {
	Input       string `json:"input"`
	Output      string `json:"output"`
	Explanation string `json:"explanation"`
}

type sourceInput struct {
	SourceID    string     `json:"source_id"`
	SourceName  string     `json:"source_name"`
	SourceURL   string     `json:"source_url"`
	PublishedAt *time.Time `json:"published_at"`
}

type optionInput struct {
	Text      string `json:"text"`
	IsCorrect bool   `json:"is_correct"`
}

type statusInput struct {
	Status string `json:"status"`
}

type submissionInput struct {
	TaskVersionID     string   `json:"task_version_id"`
	IdempotencyKey    string   `json:"idempotency_key"`
	SelectedOptionIDs []string `json:"selected_option_ids"`
}

type optionResponse struct {
	ID        string `json:"id"`
	Text      string `json:"text"`
	Position  int    `json:"position"`
	IsCorrect *bool  `json:"is_correct,omitempty"`
}

type taskResponse struct {
	ID            string           `json:"id"`
	Status        string           `json:"status"`
	TaskVersionID string           `json:"task_version_id"`
	VersionNumber int              `json:"version_number"`
	TopicID       *string          `json:"topic_id"`
	Title         string           `json:"title"`
	Statement     string           `json:"statement"`
	TaskType      string           `json:"task_type"`
	Difficulty    string           `json:"difficulty"`
	Options       []optionResponse `json:"options"`
	Tags          []string         `json:"tags"`
	Examples      []exampleInput   `json:"examples"`
	Constraints   []string         `json:"constraints"`
	Source        *sourceInput     `json:"source,omitempty"`
	CreatedAt     time.Time        `json:"created_at"`
	UpdatedAt     time.Time        `json:"updated_at"`
}

type taskSummaryResponse struct {
	ID            string    `json:"id"`
	Status        string    `json:"status"`
	TaskVersionID string    `json:"task_version_id"`
	VersionNumber int       `json:"version_number"`
	TopicID       *string   `json:"topic_id"`
	Title         string    `json:"title"`
	TaskType      string    `json:"task_type"`
	Difficulty    string    `json:"difficulty"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

type submissionResponse struct {
	ID                  string    `json:"id"`
	UserID              string    `json:"user_id"`
	TaskID              string    `json:"task_id"`
	TaskVersionID       string    `json:"task_version_id"`
	TaskVersionNumber   int       `json:"task_version_number"`
	SelectedOptionIDs   []string  `json:"selected_option_ids"`
	CorrectOptionIDs    []string  `json:"correct_option_ids"`
	Correct             bool      `json:"correct"`
	Verdict             string    `json:"verdict"`
	TaskUpdated         bool      `json:"task_updated"`
	LatestTaskVersionID string    `json:"latest_task_version_id"`
	LatestVersionNumber int       `json:"latest_version_number"`
	CreatedAt           time.Time `json:"created_at"`
}

type listResponse[T any] struct {
	Items  []T `json:"items"`
	Limit  int `json:"limit"`
	Offset int `json:"offset"`
}

// domainInput преобразует transport DTO в доменный ввод.
func (input taskInput) domainInput() (domain.TaskInput, error) {
	topicID, err := optionalUUID(input.TopicID)
	if err != nil {
		return domain.TaskInput{}, err
	}
	options := lo.Map(input.Options, func(option optionInput, _ int) domain.OptionInput {
		return domain.OptionInput{
			Text:      option.Text,
			IsCorrect: option.IsCorrect,
		}
	})

	return domain.TaskInput{
		TopicID:    topicID,
		Title:      input.Title,
		Statement:  input.Statement,
		TaskType:   domain.TaskType(input.TaskType),
		Difficulty: domain.Difficulty(input.Difficulty),
		Options:    options,
		Tags:       input.Tags,
		Examples: lo.Map(input.Examples, func(example exampleInput, _ int) domain.TaskExample {
			return domain.TaskExample{Input: example.Input, Output: example.Output, Explanation: example.Explanation}
		}),
		Constraints: input.Constraints,
		Source:      taskSource(input.Source),
	}, nil
}

// responseTask преобразует доменную задачу в public или admin DTO.
func responseTask(detail domain.TaskDetail, admin bool) taskResponse {
	options := lo.Map(detail.Version.Options, func(option domain.TaskOption, _ int) optionResponse {
		response := optionResponse{
			ID:       option.ID.String(),
			Text:     option.Text,
			Position: option.Position,
		}
		if admin {
			value := option.IsCorrect
			response.IsCorrect = &value
		}

		return response
	})

	return taskResponse{
		ID:            detail.Task.ID.String(),
		Status:        string(detail.Task.Status),
		TaskVersionID: detail.Version.ID.String(),
		VersionNumber: detail.Version.VersionNumber,
		TopicID:       optionalUUIDString(detail.Version.TopicID),
		Title:         detail.Version.Title,
		Statement:     detail.Version.Statement,
		TaskType:      string(detail.Version.TaskType),
		Difficulty:    string(detail.Version.Difficulty),
		Options:       options,
		Tags:          detail.Version.Tags,
		Examples: lo.Map(detail.Version.Examples, func(example domain.TaskExample, _ int) exampleInput {
			return exampleInput{Input: example.Input, Output: example.Output, Explanation: example.Explanation}
		}),
		Constraints: detail.Version.Constraints,
		Source:      sourceResponse(detail.Version.Source),
		CreatedAt:   detail.Task.CreatedAt,
		UpdatedAt:   detail.Task.UpdatedAt,
	}
}

// taskSource преобразует transport-атрибуцию в доменную модель.
func taskSource(input *sourceInput) *domain.TaskSource {
	if input == nil {
		return nil
	}

	return &domain.TaskSource{SourceID: input.SourceID, SourceName: input.SourceName, SourceURL: input.SourceURL, PublishedAt: input.PublishedAt}
}

// sourceResponse преобразует доменную атрибуцию в transport DTO.
func sourceResponse(source *domain.TaskSource) *sourceInput {
	if source == nil {
		return nil
	}

	return &sourceInput{SourceID: source.SourceID, SourceName: source.SourceName, SourceURL: source.SourceURL, PublishedAt: source.PublishedAt}
}

// responseTaskSummary преобразует доменную задачу в элемент списка.
func responseTaskSummary(detail domain.TaskDetail) taskSummaryResponse {
	return taskSummaryResponse{
		ID:            detail.Task.ID.String(),
		Status:        string(detail.Task.Status),
		TaskVersionID: detail.Version.ID.String(),
		VersionNumber: detail.Version.VersionNumber,
		TopicID:       optionalUUIDString(detail.Version.TopicID),
		Title:         detail.Version.Title,
		TaskType:      string(detail.Version.TaskType),
		Difficulty:    string(detail.Version.Difficulty),
		CreatedAt:     detail.Task.CreatedAt,
		UpdatedAt:     detail.Task.UpdatedAt,
	}
}

// responseSubmission преобразует доменный результат в безопасный DTO владельца.
func responseSubmission(submission domain.Submission) submissionResponse {
	return submissionResponse{
		ID:                  submission.ID.String(),
		UserID:              submission.UserID.String(),
		TaskID:              submission.TaskID.String(),
		TaskVersionID:       submission.TaskVersionID.String(),
		TaskVersionNumber:   submission.TaskVersionNumber,
		SelectedOptionIDs:   lo.Map(submission.SelectedOptionIDs, func(id uuid.UUID, _ int) string { return id.String() }),
		CorrectOptionIDs:    lo.Map(submission.CorrectOptionIDs, func(id uuid.UUID, _ int) string { return id.String() }),
		Correct:             submission.IsCorrect(),
		Verdict:             string(submission.Verdict),
		TaskUpdated:         submission.TaskUpdated,
		LatestTaskVersionID: submission.LatestTaskVersionID.String(),
		LatestVersionNumber: submission.LatestVersionNumber,
		CreatedAt:           submission.CreatedAt,
	}
}

// domainSubmissionInput преобразует transport DTO в use-case ввод.
func (input submissionInput) domainSubmissionInput() (usecase.SubmitInput, error) {
	versionID, err := uuid.Parse(input.TaskVersionID)
	if err != nil {
		return usecase.SubmitInput{}, err
	}
	key, err := uuid.Parse(input.IdempotencyKey)
	if err != nil {
		return usecase.SubmitInput{}, err
	}
	selected := make([]uuid.UUID, 0, len(input.SelectedOptionIDs))
	for _, value := range input.SelectedOptionIDs {
		id, parseErr := uuid.Parse(value)
		if parseErr != nil {
			return usecase.SubmitInput{}, parseErr
		}
		selected = append(selected, id)
	}

	return usecase.SubmitInput{
		TaskVersionID:     versionID,
		IdempotencyKey:    key,
		SelectedOptionIDs: selected,
	}, nil
}

// optionalUUID преобразует необязательную строку в UUID.
func optionalUUID(value *string) (*uuid.UUID, error) {
	if value == nil || *value == "" {
		return nil, nil
	}
	id, err := uuid.Parse(*value)
	if err != nil {
		return nil, err
	}

	return &id, nil
}

// optionalUUIDString преобразует необязательный UUID в JSON-строку.
func optionalUUIDString(value *uuid.UUID) *string {
	if value == nil {
		return nil
	}
	result := value.String()

	return &result
}

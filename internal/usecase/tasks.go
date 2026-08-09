package usecase

import (
	"context"
	"fmt"
	"net/http"

	"github.com/google/uuid"
	"github.com/overmindv/tasks-it/internal/apperror"
	"github.com/overmindv/tasks-it/internal/domain"
	"github.com/overmindv/tasks-it/internal/repository"
)

// TaskService реализует lifecycle и версионирование тестовых задач.
type TaskService struct {
	repository repository.Repository
}

// NewTaskService создаёт use-case сервис задач.
func NewTaskService(store repository.Repository) *TaskService {
	return &TaskService{
		repository: store,
	}
}

// Create создаёт draft и первую неизменяемую версию теста.
func (s *TaskService) Create(ctx context.Context, input domain.TaskInput, actorID uuid.UUID) (domain.TaskDetail, error) {
	input = domain.NormalizeTaskInput(input)
	if err := domain.ValidateTaskInput(input); err != nil {
		return domain.TaskDetail{}, validation(err)
	}
	taskID := uuid.New()
	versionID := uuid.New()
	err := s.repository.WithinTransaction(ctx, func(tx repository.Repository) error {
		task := domain.Task{
			ID:        taskID,
			Status:    domain.TaskStatusDraft,
			CreatedBy: actorID,
			UpdatedBy: actorID,
		}
		if err := tx.InsertTask(ctx, task); err != nil {
			return fmt.Errorf("insert task: %w", err)
		}
		version := buildVersion(taskID, versionID, 1, input, actorID)
		if err := tx.InsertTaskVersion(ctx, version); err != nil {
			return fmt.Errorf("insert task version: %w", err)
		}
		if err := tx.InsertTaskOptions(ctx, version.Options); err != nil {
			return fmt.Errorf("insert task options: %w", err)
		}
		if err := tx.SetCurrentTaskVersion(ctx, taskID, versionID, actorID); err != nil {
			return fmt.Errorf("set current task version: %w", err)
		}

		return nil
	})
	if err != nil {
		return domain.TaskDetail{}, fmt.Errorf("create task: %w", err)
	}

	return s.GetAdmin(ctx, taskID)
}

// Update создаёт новую текущую версию существующего теста.
func (s *TaskService) Update(ctx context.Context, taskID uuid.UUID, input domain.TaskInput, actorID uuid.UUID) (domain.TaskDetail, error) {
	input = domain.NormalizeTaskInput(input)
	if err := domain.ValidateTaskInput(input); err != nil {
		return domain.TaskDetail{}, validation(err)
	}
	err := s.repository.WithinTransaction(ctx, func(tx repository.Repository) error {
		task, err := tx.GetTask(ctx, taskID, true)
		if err != nil {
			return fmt.Errorf("get task for update: %w", err)
		}
		if task.Status == domain.TaskStatusArchived {
			return apperror.New(apperror.TaskNotAvailable, "архивный тест сначала нужно вернуть в draft", http.StatusConflict)
		}
		current, err := tx.GetCurrentTaskVersion(ctx, taskID)
		if err != nil {
			return fmt.Errorf("get current task version: %w", err)
		}
		version := buildVersion(taskID, uuid.New(), current.VersionNumber+1, input, actorID)
		if err := tx.InsertTaskVersion(ctx, version); err != nil {
			return fmt.Errorf("insert next task version: %w", err)
		}
		if err := tx.InsertTaskOptions(ctx, version.Options); err != nil {
			return fmt.Errorf("insert next task options: %w", err)
		}
		if err := tx.SetCurrentTaskVersion(ctx, taskID, version.ID, actorID); err != nil {
			return fmt.Errorf("set next current task version: %w", err)
		}

		return nil
	})
	if err != nil {
		return domain.TaskDetail{}, fmt.Errorf("update task: %w", err)
	}

	return s.GetAdmin(ctx, taskID)
}

// GetAdmin возвращает текущую версию теста для административного API.
func (s *TaskService) GetAdmin(ctx context.Context, taskID uuid.UUID) (domain.TaskDetail, error) {
	detail, err := s.repository.GetTaskDetail(ctx, taskID)
	if err != nil {
		return domain.TaskDetail{}, fmt.Errorf("get admin task: %w", err)
	}

	return detail, nil
}

// GetPublished возвращает только опубликованный тест для пользователя.
func (s *TaskService) GetPublished(ctx context.Context, taskID uuid.UUID) (domain.TaskDetail, error) {
	detail, err := s.repository.GetTaskDetail(ctx, taskID)
	if err != nil {
		return domain.TaskDetail{}, fmt.Errorf("get published task: %w", err)
	}
	if detail.Task.Status != domain.TaskStatusPublished {
		return domain.TaskDetail{}, apperror.New(apperror.TaskNotFound, "тест не найден", http.StatusNotFound)
	}

	return detail, nil
}

// ListAdmin возвращает административный список тестов.
func (s *TaskService) ListAdmin(ctx context.Context, filter domain.TaskFilter) ([]domain.TaskDetail, error) {
	filter.Limit, filter.Offset = domain.NormalizePagination(filter.Limit, filter.Offset)
	items, err := s.repository.ListTaskDetails(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("list admin tasks: %w", err)
	}

	return items, nil
}

// ListPublished возвращает опубликованные тесты с явными фильтрами.
func (s *TaskService) ListPublished(ctx context.Context, filter domain.TaskFilter) ([]domain.TaskDetail, error) {
	status := domain.TaskStatusPublished
	filter.Status = &status
	filter.Limit, filter.Offset = domain.NormalizePagination(filter.Limit, filter.Offset)
	items, err := s.repository.ListTaskDetails(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("list published tasks: %w", err)
	}

	return items, nil
}

// ChangeStatus выполняет разрешённый lifecycle-переход теста.
func (s *TaskService) ChangeStatus(ctx context.Context, taskID uuid.UUID, status domain.TaskStatus, actorID uuid.UUID) (domain.TaskDetail, error) {
	err := s.repository.WithinTransaction(ctx, func(tx repository.Repository) error {
		task, err := tx.GetTask(ctx, taskID, true)
		if err != nil {
			return fmt.Errorf("get task for status change: %w", err)
		}
		if !domain.CanTransition(task.Status, status) {
			return apperror.New(apperror.InvalidStatusTransition, "недопустимый переход статуса", http.StatusConflict)
		}
		if err := tx.SetTaskStatus(ctx, taskID, status, actorID); err != nil {
			return fmt.Errorf("set task status: %w", err)
		}

		return nil
	})
	if err != nil {
		return domain.TaskDetail{}, fmt.Errorf("change task status: %w", err)
	}

	return s.GetAdmin(ctx, taskID)
}

// Delete выполняет soft delete задачи и сохраняет её историю.
func (s *TaskService) Delete(ctx context.Context, taskID, actorID uuid.UUID) error {
	err := s.repository.SoftDeleteTask(ctx, taskID, actorID)
	if err != nil {
		return fmt.Errorf("delete task: %w", err)
	}

	return nil
}

// buildVersion создаёт доменную версию и новые ID вариантов ответа.
func buildVersion(taskID, versionID uuid.UUID, number int, input domain.TaskInput, actorID uuid.UUID) domain.TaskVersion {
	options := make([]domain.TaskOption, 0, len(input.Options))
	for position, option := range input.Options {
		options = append(options, domain.TaskOption{
			ID:            uuid.New(),
			TaskVersionID: versionID,
			Text:          option.Text,
			IsCorrect:     option.IsCorrect,
			Position:      position,
		})
	}

	return domain.TaskVersion{
		ID:            versionID,
		TaskID:        taskID,
		VersionNumber: number,
		TopicID:       input.TopicID,
		Title:         input.Title,
		Statement:     input.Statement,
		TaskType:      input.TaskType,
		Difficulty:    input.Difficulty,
		CreatedBy:     actorID,
		Options:       options,
	}
}

// validation преобразует доменную валидацию в публичную ошибку API.
func validation(err error) error {
	return apperror.New(apperror.ValidationError, err.Error(), http.StatusBadRequest)
}

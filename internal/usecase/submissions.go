package usecase

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/google/uuid"
	"github.com/overmindv/tasks-it/internal/apperror"
	"github.com/overmindv/tasks-it/internal/checker"
	"github.com/overmindv/tasks-it/internal/domain"
	"github.com/overmindv/tasks-it/internal/repository"
	"github.com/samber/lo"
)

// SubmitInput описывает версию и выбранные варианты идемпотентной отправки.
type SubmitInput struct {
	TaskVersionID     uuid.UUID
	IdempotencyKey    uuid.UUID
	SelectedOptionIDs []uuid.UUID
}

// SubmissionService проверяет ответы и управляет историей решений.
type SubmissionService struct {
	repository repository.Repository
}

// NewSubmissionService создаёт use-case сервис решений.
func NewSubmissionService(store repository.Repository) *SubmissionService {
	return &SubmissionService{
		repository: store,
	}
}

// Submit проверяет ответ по указанной версии и сохраняет результат.
func (s *SubmissionService) Submit(ctx context.Context, taskID, userID uuid.UUID, input SubmitInput) (domain.Submission, error) {
	selected := lo.Uniq(input.SelectedOptionIDs)
	if len(selected) == 0 || len(selected) != len(input.SelectedOptionIDs) {
		return domain.Submission{}, apperror.New(apperror.InvalidAnswer, "нужно выбрать уникальные варианты ответа", http.StatusBadRequest)
	}
	hash := requestHash(taskID, input.TaskVersionID, selected)
	existing, err := s.repository.FindSubmissionByIdempotency(ctx, userID, input.IdempotencyKey)
	if err != nil {
		return domain.Submission{}, fmt.Errorf("find submission by idempotency key: %w", err)
	}
	if existing != nil {
		return sameIdempotentSubmission(*existing, hash)
	}
	var submission domain.Submission
	err = s.repository.WithinTransaction(ctx, func(tx repository.Repository) error {
		task, err := tx.GetTask(ctx, taskID, true)
		if err != nil {
			return fmt.Errorf("get task for submission: %w", err)
		}
		if task.Status != domain.TaskStatusPublished {
			return apperror.New(apperror.TaskNotAvailable, "тест недоступен для решения", http.StatusConflict)
		}
		version, err := tx.GetTaskVersion(ctx, taskID, input.TaskVersionID)
		if err != nil {
			return fmt.Errorf("get submitted task version: %w", err)
		}
		if version.TaskType == domain.TaskTypeProgramming {
			return apperror.New(apperror.TaskTypeNotSubmittable, "программная задача не принимает ответы в сервисе", http.StatusConflict)
		}
		if err := validateSelectedOptions(version, selected); err != nil {
			return err
		}
		correctIDs := lo.FilterMap(version.Options, func(option domain.TaskOption, _ int) (uuid.UUID, bool) {
			return option.ID, option.IsCorrect
		})
		verdict := domain.VerdictWrongAnswer
		if checker.Choice(selected, correctIDs) {
			verdict = domain.VerdictAccepted
		}
		submission = domain.Submission{
			ID:                uuid.New(),
			UserID:            userID,
			TaskID:            taskID,
			TaskVersionID:     version.ID,
			TaskVersionNumber: version.VersionNumber,
			IdempotencyKey:    input.IdempotencyKey,
			RequestHash:       hash,
			Verdict:           verdict,
			SelectedOptionIDs: selected,
			CorrectOptionIDs:  correctIDs,
		}
		if err := tx.InsertSubmission(ctx, submission); err != nil {
			return fmt.Errorf("insert submission: %w", err)
		}
		if err := tx.InsertSubmissionAnswers(ctx, submission.ID, version.ID, selected); err != nil {
			return fmt.Errorf("insert submission answers: %w", err)
		}

		return nil
	})
	if err != nil {
		existing, findErr := s.repository.FindSubmissionByIdempotency(ctx, userID, input.IdempotencyKey)
		if findErr == nil && existing != nil {
			return sameIdempotentSubmission(*existing, hash)
		}

		return domain.Submission{}, fmt.Errorf("submit answer: %w", err)
	}

	return s.Get(ctx, submission.ID, userID, false)
}

// Get возвращает результат владельцу или администратору.
func (s *SubmissionService) Get(ctx context.Context, submissionID, actorID uuid.UUID, admin bool) (domain.Submission, error) {
	submission, err := s.repository.GetSubmission(ctx, submissionID)
	if err != nil {
		return domain.Submission{}, fmt.Errorf("get submission: %w", err)
	}
	if !admin && submission.UserID != actorID {
		return domain.Submission{}, apperror.New(apperror.PermissionDenied, "нет доступа к результату", http.StatusForbidden)
	}

	return submission, nil
}

// ListMine возвращает пагинированную историю решений пользователя.
func (s *SubmissionService) ListMine(ctx context.Context, filter domain.SubmissionFilter) ([]domain.Submission, error) {
	filter.Limit, filter.Offset = domain.NormalizePagination(filter.Limit, filter.Offset)
	items, err := s.repository.ListSubmissions(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("list user submissions: %w", err)
	}

	return items, nil
}

// validateSelectedOptions проверяет формат ответа и принадлежность вариантов версии.
func validateSelectedOptions(version domain.TaskVersion, selected []uuid.UUID) error {
	if version.TaskType == domain.TaskTypeSingleChoice && len(selected) != 1 {
		return apperror.New(apperror.InvalidAnswer, "single_choice принимает ровно один вариант", http.StatusBadRequest)
	}
	optionSet := lo.SliceToMap(version.Options, func(option domain.TaskOption) (uuid.UUID, struct{}) {
		return option.ID, struct{}{}
	})
	if !lo.EveryBy(selected, func(id uuid.UUID) bool {
		_, ok := optionSet[id]

		return ok
	}) {
		return apperror.New(apperror.InvalidAnswer, "вариант не принадлежит указанной версии", http.StatusBadRequest)
	}

	return nil
}

// requestHash строит стабильный fingerprint содержимого отправки.
func requestHash(taskID, versionID uuid.UUID, selected []uuid.UUID) string {
	values := lo.Map(selected, func(id uuid.UUID, _ int) string { return id.String() })
	slices.Sort(values)
	sum := sha256.Sum256([]byte(taskID.String() + ":" + versionID.String() + ":" + strings.Join(values, ",")))

	return hex.EncodeToString(sum[:])
}

// sameIdempotentSubmission возвращает прежний результат либо конфликт ключа.
func sameIdempotentSubmission(submission domain.Submission, hash string) (domain.Submission, error) {
	if submission.RequestHash != hash {
		return domain.Submission{}, apperror.New(apperror.IdempotencyKeyConflict, "ключ уже использован для другого ответа", http.StatusConflict)
	}

	return submission, nil
}

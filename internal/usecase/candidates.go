package usecase

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/apperror"
	"github.com/overmindv/tasks/internal/domain"
	"github.com/overmindv/tasks/internal/repository"
)

// CandidateImportResult описывает результат одного элемента batch ingestion.
type CandidateImportResult struct {
	ExternalID  string    `json:"external_id"`
	CandidateID uuid.UUID `json:"candidate_id,omitempty"`
	Status      string    `json:"status"`
	Message     string    `json:"message,omitempty"`
}

// CandidateService управляет ingestion и модерацией собранных задач.
type CandidateService struct {
	repository repository.Repository
}

// NewCandidateService создаёт use-case очереди кандидатов.
func NewCandidateService(store repository.Repository) *CandidateService {
	return &CandidateService{repository: store}
}

// ImportBatch валидирует и идемпотентно сохраняет каждый элемент независимо.
func (s *CandidateService) ImportBatch(ctx context.Context, inputs []domain.CandidateImport) []CandidateImportResult {
	results := make([]CandidateImportResult, 0, len(inputs))
	for _, raw := range inputs {
		input := domain.NormalizeCandidateImport(raw)
		result := CandidateImportResult{ExternalID: input.ExternalID}
		if err := domain.ValidateCandidateImport(input); err != nil {
			result.Status = "invalid"
			result.Message = err.Error()
			results = append(results, result)

			continue
		}
		candidate := candidateFromImport(input)
		created := false
		err := s.repository.WithinTransaction(ctx, func(tx repository.Repository) error {
			var insertErr error
			created, insertErr = tx.InsertCandidate(ctx, candidate)

			return insertErr
		})
		if err != nil {
			result.Status = "error"
			result.Message = "не удалось сохранить кандидата"
		} else if !created {
			result.Status = "duplicate"
		} else {
			result.Status = "imported"
			result.CandidateID = candidate.ID
		}
		results = append(results, result)
	}

	return results
}

// Get возвращает кандидата административному API.
func (s *CandidateService) Get(ctx context.Context, id uuid.UUID) (domain.TaskCandidate, error) {
	candidate, err := s.repository.GetCandidate(ctx, id, false)
	if err != nil {
		return domain.TaskCandidate{}, fmt.Errorf("get task candidate: %w", err)
	}

	return candidate, nil
}

// List возвращает пагинированную очередь кандидатов.
func (s *CandidateService) List(ctx context.Context, filter domain.CandidateFilter) ([]domain.TaskCandidate, error) {
	filter.Limit, filter.Offset = domain.NormalizePagination(filter.Limit, filter.Offset)
	items, err := s.repository.ListCandidates(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("list task candidates: %w", err)
	}

	return items, nil
}

// Update сохраняет правки pending-кандидата с optimistic locking.
func (s *CandidateService) Update(ctx context.Context, id uuid.UUID, review domain.CandidateReview) (domain.TaskCandidate, error) {
	review = domain.NormalizeCandidateReview(review)
	if err := domain.ValidateCandidateReview(review); err != nil {
		return domain.TaskCandidate{}, validation(err)
	}
	err := s.repository.WithinTransaction(ctx, func(tx repository.Repository) error {
		candidate, err := tx.GetCandidate(ctx, id, true)
		if err != nil {
			return fmt.Errorf("get candidate for update: %w", err)
		}
		if candidate.Status != domain.CandidateStatusPending {
			return apperror.New(apperror.CandidateNotPending, "кандидат уже обработан", http.StatusConflict)
		}
		applyReview(&candidate, review)

		return tx.UpdateCandidate(ctx, candidate, review.ExpectedRevision)
	})
	if err != nil {
		return domain.TaskCandidate{}, fmt.Errorf("update task candidate: %w", err)
	}

	return s.Get(ctx, id)
}

// Approve применяет итоговые правки и атомарно создаёт опубликованную programming-задачу.
func (s *CandidateService) Approve(ctx context.Context, id, actorID uuid.UUID, review domain.CandidateReview) (domain.TaskDetail, error) {
	review = domain.NormalizeCandidateReview(review)
	if err := domain.ValidateCandidateReview(review); err != nil {
		return domain.TaskDetail{}, validation(err)
	}
	var taskID uuid.UUID
	err := s.repository.WithinTransaction(ctx, func(tx repository.Repository) error {
		candidate, err := tx.GetCandidate(ctx, id, true)
		if err != nil {
			return fmt.Errorf("get candidate for approval: %w", err)
		}
		if candidate.Status == domain.CandidateStatusApproved && candidate.ApprovedTaskID != nil {
			taskID = *candidate.ApprovedTaskID

			return nil
		}
		if candidate.Status != domain.CandidateStatusPending {
			return apperror.New(apperror.CandidateNotPending, "кандидат уже отклонён", http.StatusConflict)
		}
		if candidate.Revision != review.ExpectedRevision {
			return apperror.New(apperror.CandidateRevisionConflict, "кандидат был изменён другим администратором", http.StatusConflict)
		}
		applyReview(&candidate, review)
		if err := tx.UpdateCandidate(ctx, candidate, review.ExpectedRevision); err != nil {
			return fmt.Errorf("save final candidate payload: %w", err)
		}
		taskID = uuid.New()
		versionID := uuid.New()
		task := domain.Task{ID: taskID, Status: domain.TaskStatusPublished, CreatedBy: actorID, UpdatedBy: actorID}
		if err := tx.InsertTask(ctx, task); err != nil {
			return fmt.Errorf("insert approved task: %w", err)
		}
		version := domain.TaskVersion{
			ID: versionID, TaskID: taskID, VersionNumber: 1, TopicID: candidate.TopicID,
			Title: candidate.Title, Statement: candidate.Statement, TaskType: domain.TaskTypeProgramming,
			Difficulty: candidate.Difficulty, CreatedBy: actorID, Tags: candidate.Tags,
			Examples: candidate.Examples, Constraints: candidate.Constraints,
			Source: &domain.TaskSource{SourceID: candidate.SourceID, SourceName: candidate.SourceName,
				SourceURL: candidate.SourceURL, PublishedAt: candidate.SourcePublishedAt},
		}
		if err := tx.InsertTaskVersion(ctx, version); err != nil {
			return fmt.Errorf("insert approved task version: %w", err)
		}
		if err := tx.InsertTaskContent(ctx, version); err != nil {
			return fmt.Errorf("insert approved task content: %w", err)
		}
		if err := tx.SetCurrentTaskVersion(ctx, taskID, versionID, actorID); err != nil {
			return fmt.Errorf("set approved task version: %w", err)
		}
		if err := tx.MarkCandidateApproved(ctx, candidate.ID, taskID, actorID, review.ExpectedRevision+1); err != nil {
			return fmt.Errorf("mark candidate approved: %w", err)
		}

		return nil
	})
	if err != nil {
		return domain.TaskDetail{}, fmt.Errorf("approve task candidate: %w", err)
	}
	detail, err := s.repository.GetTaskDetail(ctx, taskID)
	if err != nil {
		return domain.TaskDetail{}, fmt.Errorf("get approved task: %w", err)
	}

	return detail, nil
}

// Reject завершает pending-кандидата без создания задачи.
func (s *CandidateService) Reject(ctx context.Context, id, actorID uuid.UUID, expectedRevision int, reason string) (domain.TaskCandidate, error) {
	reason = strings.TrimSpace(reason)
	if expectedRevision <= 0 || len([]rune(reason)) > 1000 {
		return domain.TaskCandidate{}, validation(fmt.Errorf("expected_revision обязателен, причина не должна превышать 1000 символов"))
	}
	err := s.repository.WithinTransaction(ctx, func(tx repository.Repository) error {
		candidate, err := tx.GetCandidate(ctx, id, true)
		if err != nil {
			return fmt.Errorf("get candidate for reject: %w", err)
		}
		if candidate.Status != domain.CandidateStatusPending {
			return apperror.New(apperror.CandidateNotPending, "кандидат уже обработан", http.StatusConflict)
		}

		return tx.MarkCandidateRejected(ctx, id, actorID, expectedRevision, reason)
	})
	if err != nil {
		return domain.TaskCandidate{}, fmt.Errorf("reject task candidate: %w", err)
	}

	return s.Get(ctx, id)
}

// candidateFromImport создаёт pending-кандидата с первой revision.
func candidateFromImport(input domain.CandidateImport) domain.TaskCandidate {
	return domain.TaskCandidate{
		ID: uuid.New(), Status: domain.CandidateStatusPending, Revision: 1, ExternalID: input.ExternalID,
		SourceID: input.SourceID, SourceName: input.SourceName, SourceURL: input.SourceURL,
		SourceHash: input.SourceHash, SourcePublishedAt: input.SourcePublishedAt, RetrievedAt: input.RetrievedAt,
		CollectionJobID: input.CollectionJobID, Title: input.Title, Statement: input.Statement,
		Difficulty: input.Difficulty, Tags: input.Tags, Examples: input.Examples, Constraints: input.Constraints,
	}
}

// applyReview переносит редактируемые поля в кандидата.
func applyReview(candidate *domain.TaskCandidate, review domain.CandidateReview) {
	candidate.TopicID = review.TopicID
	candidate.Title = review.Title
	candidate.Statement = review.Statement
	candidate.Difficulty = review.Difficulty
	candidate.Tags = review.Tags
	candidate.Examples = review.Examples
	candidate.Constraints = review.Constraints
}

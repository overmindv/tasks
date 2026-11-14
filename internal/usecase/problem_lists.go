package usecase

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/domain"
	"github.com/overmindv/tasks/internal/repository"
)

// ProblemListService управляет сущностью «список задач».
type ProblemListService struct {
	repository repository.Repository
}

// NewProblemListService создаёт use-case списков задач.
func NewProblemListService(store repository.Repository) *ProblemListService {
	return &ProblemListService{repository: store}
}

// List возвращает пагинированный журнал списков задач.
func (s *ProblemListService) List(ctx context.Context, limit, offset int) ([]domain.ProblemListSummary, error) {
	limit, offset = domain.NormalizePagination(limit, offset)
	items, err := s.repository.ListProblemLists(ctx, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("list problem lists: %w", err)
	}

	return items, nil
}

// Get возвращает детали списка вместе с опубликованными членами.
func (s *ProblemListService) Get(ctx context.Context, id uuid.UUID) (domain.ProblemListDetail, error) {
	detail, err := s.repository.GetProblemListDetail(ctx, id)
	if err != nil {
		return domain.ProblemListDetail{}, fmt.Errorf("get problem list: %w", err)
	}

	return detail, nil
}

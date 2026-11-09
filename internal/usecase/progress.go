package usecase

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/domain"
)

var ErrProgressPrivate = errors.New("practice progress is private")

type ProgressStore interface {
	GetProgressVisibility(context.Context, uuid.UUID) (bool, error)
	SetProgressVisibility(context.Context, uuid.UUID, bool) error
	VisitGradedAttempts(context.Context, uuid.UUID, func(domain.GradedAttempt) error) error
	ListRatedUsers(context.Context, *[]uuid.UUID, int, int) ([]domain.RatedUser, error)
}

type ProgressService struct{ store ProgressStore }

func NewProgressService(store ProgressStore) *ProgressService { return &ProgressService{store: store} }

// Get возвращает собственную статистику всегда, чужую — только по явному opt-in.
func (s *ProgressService) Get(ctx context.Context, actorID, userID uuid.UUID) (domain.ProgressSummary, error) {
	public, err := s.store.GetProgressVisibility(ctx, userID)
	if err != nil {
		return domain.ProgressSummary{}, err
	}
	if actorID != userID && !public {
		return domain.ProgressSummary{}, ErrProgressPrivate
	}
	progress := domain.NewProgressAccumulator(userID, public, time.Now())
	if err := s.store.VisitGradedAttempts(ctx, userID, func(attempt domain.GradedAttempt) error {
		progress.Add(attempt)
		return nil
	}); err != nil {
		return domain.ProgressSummary{}, err
	}
	return progress.Summary(), nil
}

func (s *ProgressService) SetVisibility(ctx context.Context, userID uuid.UUID, public bool) error {
	return s.store.SetProgressVisibility(ctx, userID, public)
}

func (s *ProgressService) Candidates(ctx context.Context, topicIDs *[]uuid.UUID, limit, offset int) ([]domain.RatedUser, error) {
	return s.store.ListRatedUsers(ctx, topicIDs, limit, offset)
}

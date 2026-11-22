package usecase

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/domain"
)

type PrivacyStore interface {
	OutcomeManifest(context.Context, int64, int64) (domain.OutcomeManifest, error)
	OutcomeReplay(context.Context, int64, int64, int64, int) (domain.OutcomeReplayPage, error)
	AccountExport(context.Context, uuid.UUID, string, string, int) (domain.AccountExportPage, error)
	ApplyAccountDeletion(context.Context, domain.AccountDeletion, int) (bool, error)
	PrivacyPosition(context.Context) (int64, error)
	AdvancePrivacyPosition(context.Context, int64) error
	ExpireLearningData(context.Context, time.Time, time.Time, int) error
	ExpireAccountExports(context.Context) error
}
type DeletionPage struct {
	Watermark int64
	NextAfter int64
	Events    []domain.AccountDeletion
}
type PrivacyLedger interface {
	Read(context.Context, int64) (DeletionPage, error)
	Ack(context.Context, uuid.UUID) error
}
type PrivacyService struct {
	PrivacyStore
	Ledger                         PrivacyLedger
	RawRetention, OutcomeRetention time.Duration
}

// SyncOnce продвигает cursor только после локального удаления и подтверждения Users.
func (s *PrivacyService) SyncOnce(ctx context.Context) error {
	if err := s.ExpireAccountExports(ctx); err != nil {
		return err
	}
	var raw, history time.Time
	now := time.Now().UTC()
	if s.RawRetention > 0 {
		raw = now.Add(-s.RawRetention)
	}
	if s.OutcomeRetention > 0 {
		history = now.Add(-s.OutcomeRetention)
	}
	if !raw.IsZero() || !history.IsZero() {
		if err := s.ExpireLearningData(ctx, raw, history, 100); err != nil {
			return err
		}
	}
	if s.Ledger == nil {
		return nil
	}
	after, err := s.PrivacyPosition(ctx)
	if err != nil {
		return err
	}
	page, err := s.Ledger.Read(ctx, after)
	if err != nil {
		return err
	}
	if page.NextAfter < after || page.NextAfter > page.Watermark || len(page.Events) > 1 {
		return domain.ErrOutcomeSnapshot
	}
	for _, event := range page.Events {
		done, err := s.ApplyAccountDeletion(ctx, event, 100)
		if err != nil {
			return err
		}
		if !done {
			return nil
		}
		if err = s.Ledger.Ack(ctx, event.EventID); err != nil {
			return err
		}
	}

	return s.AdvancePrivacyPosition(ctx, page.NextAfter)
}

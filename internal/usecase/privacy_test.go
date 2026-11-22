package usecase

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/domain"
)

type privacyFixture struct {
	PrivacyStore
	position               int64
	done                   bool
	ackErr                 error
	applied, acks, expired int
}

func (f *privacyFixture) PrivacyPosition(context.Context) (int64, error) { return f.position, nil }
func (f *privacyFixture) AdvancePrivacyPosition(_ context.Context, p int64) error {
	f.position = p

	return nil
}
func (f *privacyFixture) ExpireAccountExports(context.Context) error { return nil }
func (f *privacyFixture) ExpireLearningData(context.Context, time.Time, time.Time, int) error {
	f.expired++

	return nil
}
func (f *privacyFixture) ApplyAccountDeletion(context.Context, domain.AccountDeletion, int) (bool, error) {
	f.applied++

	return f.done, nil
}
func (f *privacyFixture) Read(context.Context, int64) (DeletionPage, error) {
	return DeletionPage{Watermark: 1, NextAfter: 1, Events: []domain.AccountDeletion{{EventID: uuid.New(), UserID: uuid.New(), Version: 1}}}, nil
}
func (f *privacyFixture) Ack(context.Context, uuid.UUID) error {
	f.acks++

	return f.ackErr
}
func TestPrivacyCursorWaitsForDeletionAndAck(t *testing.T) {
	t.Parallel()
	f := &privacyFixture{}
	s := &PrivacyService{PrivacyStore: f, Ledger: f}
	if err := s.SyncOnce(t.Context()); err != nil || f.position != 0 || f.acks != 0 || f.expired != 0 {
		t.Fatal("premature progress", f, err)
	}
	f.done = true
	f.ackErr = errors.New("outage")
	if err := s.SyncOnce(t.Context()); err == nil || f.position != 0 {
		t.Fatal("ack failure advanced cursor")
	}
	f.ackErr = nil
	if err := s.SyncOnce(t.Context()); err != nil || f.position != 1 || f.applied != 3 || f.acks != 2 {
		t.Fatal("retry failed", f, err)
	}
}

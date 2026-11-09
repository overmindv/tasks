package usecase

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/domain"
)

type fakeProgressStore struct {
	public   map[uuid.UUID]bool
	attempts []domain.GradedAttempt
	visited  int
}

func (f *fakeProgressStore) GetProgressVisibility(_ context.Context, userID uuid.UUID) (bool, error) {
	return f.public[userID], nil
}
func (f *fakeProgressStore) SetProgressVisibility(_ context.Context, userID uuid.UUID, public bool) error {
	f.public[userID] = public
	return nil
}
func (f *fakeProgressStore) VisitGradedAttempts(_ context.Context, _ uuid.UUID, visit func(domain.GradedAttempt) error) error {
	f.visited++
	for _, item := range f.attempts {
		if err := visit(item); err != nil {
			return err
		}
	}
	return nil
}
func (f *fakeProgressStore) ListRatedUsers(_ context.Context, _ *[]uuid.UUID, _, _ int) ([]domain.RatedUser, error) {
	return nil, nil
}

func TestProgressServicePrivateThenPublic(t *testing.T) {
	owner, other := uuid.New(), uuid.New()
	store := &fakeProgressStore{public: map[uuid.UUID]bool{}, attempts: []domain.GradedAttempt{{TaskID: uuid.New(), Difficulty: "easy", Verdict: "accepted", OccurredAt: time.Now()}}}
	service := NewProgressService(store)
	if _, err := service.Get(context.Background(), other, owner); !errors.Is(err, ErrProgressPrivate) || store.visited != 0 {
		t.Fatalf("private summary leaked, err=%v visited=%d", err, store.visited)
	}
	mine, err := service.Get(context.Background(), owner, owner)
	if err != nil || mine.Score != 10 || mine.Public {
		t.Fatalf("owner summary=%+v err=%v", mine, err)
	}
	if err := service.SetVisibility(context.Background(), owner, true); err != nil {
		t.Fatal(err)
	}
	peer, err := service.Get(context.Background(), other, owner)
	if err != nil || !peer.Public || peer.Score != 10 {
		t.Fatalf("public summary=%+v err=%v", peer, err)
	}
	if err := service.SetVisibility(context.Background(), owner, false); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Get(context.Background(), other, owner); !errors.Is(err, ErrProgressPrivate) {
		t.Fatalf("revoked summary visible: %v", err)
	}
}

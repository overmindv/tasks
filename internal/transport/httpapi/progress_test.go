package httpapi

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/domain"
	"github.com/overmindv/tasks/internal/usecase"
)

type progressMemory struct {
	visible map[uuid.UUID]bool
	visited int
}

func (m *progressMemory) GetProgressVisibility(_ context.Context, id uuid.UUID) (bool, error) {
	return m.visible[id], nil
}
func (m *progressMemory) SetProgressVisibility(_ context.Context, id uuid.UUID, value bool) error {
	m.visible[id] = value
	return nil
}
func (m *progressMemory) VisitGradedAttempts(_ context.Context, _ uuid.UUID, _ func(domain.GradedAttempt) error) error {
	m.visited++
	return nil
}
func (m *progressMemory) ListRatedUsers(_ context.Context, _ *[]uuid.UUID, _, _ int) ([]domain.RatedUser, error) {
	return []domain.RatedUser{}, nil
}

func TestProgressHTTPRequiresActorAndKeepsPeerPrivate(t *testing.T) {
	store := &progressMemory{visible: make(map[uuid.UUID]bool)}
	mux := http.NewServeMux()
	RegisterProgress(mux, usecase.NewProgressService(store), slog.New(slog.NewTextHandler(io.Discard, nil)))
	owner, peer := uuid.New(), uuid.New()
	request := func(method, path string, actor uuid.UUID, body []byte) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRequest(method, path, bytes.NewReader(body))
		if actor != uuid.Nil {
			r.Header.Set("X-User-ID", actor.String())
		}
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		return w
	}
	if got := request(http.MethodGet, "/v1/me/practice-progress", uuid.Nil, nil); got.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated read=%d", got.Code)
	}
	if got := request(http.MethodGet, "/v1/users/"+owner.String()+"/practice-progress", peer, nil); got.Code != http.StatusNotFound || store.visited != 0 {
		t.Fatalf("private read=%d visited=%d", got.Code, store.visited)
	}
	if got := request(http.MethodPut, "/v1/me/practice-progress/visibility", peer, []byte(`{"public":true}`)); got.Code != http.StatusOK {
		t.Fatalf("opt-in=%d: %s", got.Code, got.Body.String())
	}
	if !store.visible[peer] || store.visible[owner] {
		t.Fatal("actor crossed on visibility update")
	}
	if got := request(http.MethodGet, "/v1/users/"+peer.String()+"/practice-progress", owner, nil); got.Code != http.StatusOK || store.visited != 1 {
		t.Fatalf("public read=%d visited=%d", got.Code, store.visited)
	}
	if got := request(http.MethodPut, "/v1/me/practice-progress/visibility", peer, []byte(`{}`)); got.Code != http.StatusBadRequest {
		t.Fatalf("missing public accepted: %d", got.Code)
	}
	if got := request(http.MethodGet, "/v1/practice-leaderboard?scope=course&topic_id=not-uuid", peer, nil); got.Code != http.StatusBadRequest {
		t.Fatalf("invalid topic accepted: %d", got.Code)
	}
}

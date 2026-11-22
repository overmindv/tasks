package httpapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/overmindv/tasks/internal/domain"
	"github.com/overmindv/tasks/internal/usecase"
)

type replayHTTPFixture struct{ usecase.PrivacyStore }

func (replayHTTPFixture) OutcomeManifest(context.Context, int64, int64) (domain.OutcomeManifest, error) {
	return domain.OutcomeManifest{Watermark: 5, PrivacyEpoch: 1}, nil
}
func TestPrivacyReplayRequiresDedicatedToken(t *testing.T) {
	t.Parallel()
	token := strings.Repeat("r", 32)
	privacy := strings.Repeat("p", 32)
	mux := http.NewServeMux()
	RegisterPrivacy(mux, &usecase.PrivacyService{PrivacyStore: replayHTTPFixture{}}, token, privacy)
	for _, tc := range []struct {
		auth   string
		status int
	}{{"", 403}, {"Bearer " + privacy, 403}, {"Bearer " + token, 200}} {
		r := httptest.NewRequest(http.MethodGet, "/v1/internal/outcomes/manifest", nil)
		r.Header.Set("Authorization", tc.auth)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		if w.Code != tc.status || w.Header().Get("Cache-Control") != "no-store" {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	r := httptest.NewRequest(http.MethodGet, "/v1/internal/outcomes/replay?limit=1000000", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	if w.Code != 409 {
		t.Fatal(w.Code)
	}
}

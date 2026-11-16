package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
)

func TestReplayRequiresAdmin(t *testing.T) {
	for _, role := range []string{"", "user"} {
		req := httptest.NewRequest(http.MethodPost, "/v1/admin/code-submissions/"+uuid.NewString()+"/replay", nil)
		if role != "" {
			req.Header.Set("X-User-ID", uuid.NewString())
			req.Header.Set("X-User-Roles", role)
		}
		w := httptest.NewRecorder()
		(&Handler{}).replayCode(w, req)
		want := http.StatusForbidden
		if role == "" {
			want = http.StatusUnauthorized
		}
		if w.Code != want {
			t.Fatalf("role %q: %d", role, w.Code)
		}
	}
}

package httpapi

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
)

func TestSuiteRoutesRequireAdmin(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	RegisterSuites(mux, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	for _, route := range []struct{ method, path string }{{"POST", "/v1/admin/tasks/" + uuid.NewString() + "/suite-drafts"}, {"GET", "/v1/admin/suite-drafts/" + uuid.NewString()}, {"PUT", "/v1/admin/suite-drafts/" + uuid.NewString()}, {"DELETE", "/v1/admin/suite-drafts/" + uuid.NewString()}, {"POST", "/v1/admin/suite-drafts/" + uuid.NewString() + "/publish"}} {
		for _, role := range []string{"", "student", "teacher"} {
			r := httptest.NewRequest(route.method, route.path, nil)
			if role != "" {
				r.Header.Set("X-User-ID", uuid.NewString())
				r.Header.Set("X-User-Roles", role)
			}
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, r)
			want := 403
			if role == "" {
				want = 401
			}
			if w.Code != want {
				t.Fatalf("%s %s role %s: %d", route.method, route.path, role, w.Code)
			}
		}
	}
}

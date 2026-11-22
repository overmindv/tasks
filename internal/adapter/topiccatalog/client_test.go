package topiccatalog

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
)

func TestTopicVisibility(t *testing.T) {
	t.Parallel()
	id := uuid.New()
	for _, tc := range []struct {
		name   string
		status int
		body   string
		ok     bool
	}{
		{"active", 200, fmt.Sprintf(`{"id":%q,"status":"active"}`, id), true},
		{"hidden", 200, fmt.Sprintf(`{"id":%q,"status":"hidden"}`, id), false},
		{"wrong identity", 200, fmt.Sprintf(`{"id":%q,"status":"active"}`, uuid.New()), false},
		{"missing", 404, `{}`, false}, {"offline", 503, `{}`, false}, {"invalid", 200, `{`, false}, {"trailing", 200, `{} {}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/topics/"+id.String() || r.Header.Get("X-User-Roles") != "" {
					t.Error("incorrect public request")
				}
				w.WriteHeader(tc.status)
				_, _ = fmt.Fprint(w, tc.body)
			}))
			defer server.Close()
			c, err := New(server.URL)
			if err != nil {
				t.Fatal(err)
			}
			if err := c.ValidateTopic(context.Background(), id); (err == nil) != tc.ok {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

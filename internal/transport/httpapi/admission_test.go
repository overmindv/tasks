package httpapi

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"testing"

	"github.com/overmindv/tasks/internal/apperror"
)

func TestAdmissionRetryAfter(t *testing.T) {
	w := httptest.NewRecorder()
	err := &apperror.Error{Code: apperror.RateLimited, Message: "submission capacity unavailable", Status: 429, RetryAfterSeconds: 15}
	writeError(w, fmt.Errorf("submit: %w", err), slog.New(slog.NewTextHandler(io.Discard, nil)))
	var payload map[string]any
	if e := json.Unmarshal(w.Body.Bytes(), &payload); e != nil {
		t.Fatal(e)
	}
	if w.Code != 429 || w.Header().Get("Retry-After") != "15" || payload["code"] != "RATE_LIMITED" || payload["retry_after_seconds"] != float64(15) {
		t.Fatal("unsafe or incomplete rate limit response")
	}
}

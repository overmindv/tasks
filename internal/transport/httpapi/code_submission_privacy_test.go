package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/domain"
	"github.com/overmindv/tasks/internal/repository"
	"github.com/overmindv/tasks/internal/usecase"
)

const (
	adminKind = "admin"
	otherKind = "other"
)

type privacyRepository struct {
	repository.Repository
	submission domain.CodeSubmission
}

func (r *privacyRepository) GetCodeSubmission(context.Context, uuid.UUID) (domain.CodeSubmission, error) {
	return r.submission, nil
}
func (r *privacyRepository) ListCodeSubmissions(context.Context, domain.CodeSubmissionFilter) ([]domain.CodeSubmission, error) {
	return []domain.CodeSubmission{r.submission}, nil
}

func TestCodeSubmissionPrivacyAcrossRoutes(t *testing.T) {
	t.Parallel()

	secret := "HIDDEN_" + strings.Repeat("INPUT_9f31", 1)
	verdict := domain.ExecutionVerdictCheckerError
	repo := &privacyRepository{submission: domain.CodeSubmission{ID: uuid.New(), UserID: uuid.New(), Status: domain.CodeSubmissionStatusCompleted, Verdict: &verdict,
		Tests:       []domain.ExecutionTestResult{{TestID: "open-1", Stdout: secret, Stderr: secret, MemoryBytes: 2048}},
		Execution:   &domain.ExecutionPhaseResult{Stdout: secret, Stderr: secret, DurationMS: 42},
		Compilation: &domain.ExecutionPhaseResult{Stdout: secret, Stderr: secret},
		Failure:     &domain.ExecutionFailure{Code: secret, Message: "https://artifact.invalid/" + secret},
	}}
	mux := http.NewServeMux()
	Register(mux, nil, nil, usecase.NewCodeSubmissionService(repo, usecase.CodeExecutionPolicy{}), nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), "token")
	for _, kind := range []string{"owner", adminKind, "history", "other"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()

			path := "/v1/code-submissions/" + repo.submission.ID.String()
			id := repo.submission.UserID.String()
			if kind == "history" {
				path = "/v1/me/code-submissions"
			}
			if kind == adminKind || kind == otherKind {
				id = uuid.NewString()
			}
			req := httptest.NewRequest(http.MethodGet, path, nil)
			req.Header.Set("X-User-ID", id)
			if kind == adminKind {
				req.Header.Set("X-User-Roles", adminKind)
			}
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			want := http.StatusOK
			if kind == otherKind {
				want = http.StatusForbidden
			}
			if rec.Code != want {
				t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
			}
			if strings.Contains(rec.Body.String(), secret) || strings.Contains(rec.Body.String(), "artifact.invalid") {
				t.Fatalf("hidden output leaked: %s", rec.Body.String())
			}
			if kind != "other" {
				if !strings.Contains(rec.Body.String(), `"tests":[]`) || !strings.Contains(rec.Body.String(), `"duration_ms":42`) {
					t.Fatalf("aggregate/metrics lost: %s", rec.Body.String())
				}
			}
		})
	}
	// DTO защищает также будущие пути, минующие usecase.Get.
	raw, err := json.Marshal(responseCodeSubmission(repo.submission))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), secret) {
		t.Fatal("direct DTO leaks hidden output")
	}
	if repo.submission.Tests[0].Stdout != secret || repo.submission.Execution.Stdout != secret {
		t.Fatal("projection mutated stored result")
	}
}

func TestCodeSubmissionDTOAllowsOnlyMarkedSamples(t *testing.T) {
	t.Parallel()

	v := domain.ExecutionVerdictAccepted
	s := domain.CodeSubmission{Verdict: &v, Tests: []domain.ExecutionTestResult{
		{TestID: "open-1", SampleVisible: true, Stdout: "public sample output", Stderr: "public sample stderr", Verdict: v},
		{TestID: "hidden-1", Stdout: "HIDDEN_INPUT", Verdict: v},
	}}
	raw, err := json.Marshal(responseCodeSubmission(s))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "public sample output") || strings.Contains(string(raw), "HIDDEN_INPUT") || strings.Contains(string(raw), "sample_visible") {
		t.Fatalf("bad public projection: %s", raw)
	}
}

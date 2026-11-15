package usecase

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/overmindv/tasks/internal/domain"
	"github.com/overmindv/tasks/internal/execution"
)

func TestRunnerFailureDetailsNeverPersist(t *testing.T) {
	repo, svc, s := fencingSubmission(t)
	r := fencingResult(s, "invalid_request")
	r.Cases = nil
	r.Summary.CasesTotal = 0
	r.Error = &execution.RunError{Code: "HIDDEN_INPUT", Message: "https://artifact.invalid/HIDDEN_INPUT", Details: map[string]any{"expected": "HIDDEN_INPUT"}}
	if err := svc.HandleRunResult(context.Background(), fencingMeta(1), r); err != nil {
		t.Fatal(err)
	}
	got := repo.submissions[s.ID]
	if got.Verdict == nil || *got.Verdict != domain.ExecutionVerdictCheckerError {
		t.Fatal("checker failure classification lost")
	}
	raw, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "HIDDEN_INPUT") || strings.Contains(string(raw), "artifact.invalid") {
		t.Fatalf("persisted raw failure: %s", raw)
	}
}

func TestAcceptedSampleReceivesTrustedVisibility(t *testing.T) {
	repo, svc, s := fencingSubmission(t)
	if err := svc.HandleRunResult(context.Background(), fencingMeta(1), fencingResult(s, "ok")); err != nil {
		t.Fatal(err)
	}
	got, err := svc.Get(context.Background(), s.ID, s.UserID, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Tests) != 1 || !got.Tests[0].SampleVisible || got.Tests[0].Stdout != "hello\n" {
		t.Fatalf("sample feedback lost: %#v", got.Tests)
	}
	if repo.submissions[s.ID].SourceCode == "" || got.SourceCode != "" {
		t.Fatal("source code was mutated or leaked")
	}
}

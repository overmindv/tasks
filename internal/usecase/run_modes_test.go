package usecase

import (
	"testing"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/domain"
	"github.com/overmindv/tasks/internal/execution"
)

func TestRunModeHash(t *testing.T) {
	t.Parallel()

	id := uuid.New()
	input := CodeSubmissionInput{TaskVersionID: uuid.New(), Language: "python", SourceFileName: "a.py", SourceCode: "print(1)"}
	legacy := legacyCodeRequestHash(id, input)
	input.RunMode = domain.RunModeSamples
	sample := codeRequestHash(id, input)
	input.RunMode = domain.RunModeGraded
	if sample == codeRequestHash(id, input) || sample == legacy {
		t.Fatal("mode not fingerprinted")
	}
}

func TestLegacyModeIdempotency(t *testing.T) {
	t.Parallel()

	input := CodeSubmissionInput{TaskVersionID: uuid.New(), Language: "python", SourceCode: "print(1)"}
	id := uuid.New()
	legacy := legacyCodeRequestHash(id, input)
	old := domain.CodeSubmission{ID: uuid.New(), RequestHash: legacy, RunMode: domain.RunModeSamples}
	got, err := sameModeSubmission(old, codeRequestHash(id, input), legacy, domain.RunModeSamples)
	if err != nil || got.ID != old.ID {
		t.Fatal("legacy samples lost idempotency")
	}
	input.RunMode = domain.RunModeGraded
	if _, err := sameModeSubmission(old, codeRequestHash(id, input), legacy, domain.RunModeGraded); err == nil {
		t.Fatal("legacy key reused for graded")
	}
}
func TestGradedCheckerAndPrivacy(t *testing.T) {
	t.Parallel()

	suite := &domain.PublishedSuite{Definition: domain.SuiteDefinition{Checker: "exact", Cases: []domain.SuiteCase{{ID: "hidden", Input: "secret", Output: "ok", Hidden: true}}}}
	r := execution.RunResult{Status: "ok", Summary: execution.ResultSummary{CasesTotal: 1}, Cases: []execution.CaseRunResult{{Index: 0, Status: "ok", Stdout: "ok\n", Stderr: "secret"}}}
	verdict, tests, _ := verdictFromSuiteResult(r, suite)
	if verdict != domain.ExecutionVerdictWrongAnswer || tests[0].Stdout != "" || tests[0].Stderr != "" || tests[0].SampleVisible {
		t.Fatal("exact or hidden privacy broken")
	}
	suite.Definition.Checker = "trim_newlines"
	verdict, _, _ = verdictFromSuiteResult(r, suite)
	if verdict != domain.ExecutionVerdictAccepted {
		t.Fatal("trim checker rejected newline")
	}
	r.Summary.CasesTotal = 0
	r.Cases = nil
	verdict, _, _ = verdictFromSuiteResult(r, suite)
	if verdict != domain.ExecutionVerdictInfrastructureError {
		t.Fatal("missing graded cases accepted")
	}
}

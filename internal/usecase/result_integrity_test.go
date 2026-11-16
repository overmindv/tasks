package usecase

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/domain"
	"github.com/overmindv/tasks/internal/execution"
)

func TestResultIntegrity(t *testing.T) {
	expected := []domain.TaskExample{{Input: "a", Output: "1"}, {Input: "b", Output: "2"}}
	tests := []struct {
		name   string
		mutate func(*execution.RunResult)
		want   domain.ExecutionVerdict
	}{
		{"complete", func(r *execution.RunResult) {}, domain.ExecutionVerdictAccepted},
		{"reordered", func(r *execution.RunResult) { r.Cases[0], r.Cases[1] = r.Cases[1], r.Cases[0] }, domain.ExecutionVerdictAccepted},
		{"missing", func(r *execution.RunResult) { r.Cases = r.Cases[:1]; r.Summary.CasesTotal = 1 }, domain.ExecutionVerdictInfrastructureError},
		{"duplicate", func(r *execution.RunResult) { r.Cases[1] = r.Cases[0] }, domain.ExecutionVerdictInfrastructureError},
		{"unknown index", func(r *execution.RunResult) { r.Cases[1].Index = 2; r.Cases[1].Stdout = "" }, domain.ExecutionVerdictInfrastructureError},
		{"unknown status", func(r *execution.RunResult) { r.Cases[1].Status = "mystery" }, domain.ExecutionVerdictInfrastructureError},
		{"summary total", func(r *execution.RunResult) { r.Summary.CasesTotal = 1 }, domain.ExecutionVerdictInfrastructureError},
		{"summary errors", func(r *execution.RunResult) { r.Summary.CasesWithError = 1 }, domain.ExecutionVerdictInfrastructureError},
		{"run failure", func(r *execution.RunResult) { r.Status = "internal_error" }, domain.ExecutionVerdictInfrastructureError},
		{"run timeout", func(r *execution.RunResult) { r.Status = "tle" }, domain.ExecutionVerdictTimeLimitExceeded},
		{"run checker", func(r *execution.RunResult) { r.Status = "invalid_request" }, domain.ExecutionVerdictCheckerError},
		{"error with ok", func(r *execution.RunResult) { r.Error = &execution.RunError{Code: "broken"} }, domain.ExecutionVerdictInfrastructureError},
		{"signal", func(r *execution.RunResult) { s := "SIGKILL"; r.Cases[1].Signal = &s }, domain.ExecutionVerdictRuntimeError},
		{"truncated", func(r *execution.RunResult) { r.Cases[1].Truncated = true }, domain.ExecutionVerdictOutputLimitExceeded},
		{"wrong answer", func(r *execution.RunResult) { r.Cases[1].Stdout = "3" }, domain.ExecutionVerdictWrongAnswer},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := execution.RunResult{Status: "ok", Summary: execution.ResultSummary{CasesTotal: 2}, Cases: []execution.CaseRunResult{{Index: 0, Status: "ok", Stdout: "1"}, {Index: 1, Status: "ok", Stdout: "2"}}}
			tt.mutate(&r)
			got, _, _ := verdictFromRunResult(r, expected)
			if got != tt.want {
				t.Fatalf("verdict=%s want %s", got, tt.want)
			}
		})
	}
}

func TestRunFailureCannotMaskCaseInfrastructureError(t *testing.T) {
	r := execution.RunResult{Status: "runtime_error", Summary: execution.ResultSummary{CasesTotal: 1, CasesWithError: 1}, Cases: []execution.CaseRunResult{{Index: 0, Status: "internal_error"}}}
	got, _, _ := verdictFromRunResult(r, []domain.TaskExample{{Input: "a", Output: "1"}})
	if got != domain.ExecutionVerdictInfrastructureError {
		t.Fatalf("got %s", got)
	}
}

func TestResultContextMismatchRetriesWithoutAccepting(t *testing.T) {
	for _, kind := range []string{"missing", "version", "checksum"} {
		t.Run(kind, func(t *testing.T) {
			repo, svc, s := fencingSubmission(t)
			result := fencingResult(s, "ok")
			switch kind {
			case "missing":
				result.EvaluationContext = nil
			case "version":
				result.EvaluationContext.TaskVersionID = uuid.NewString()
			case "checksum":
				result.EvaluationContext.SuiteSHA256 = strings.Repeat("0", 64)
			}
			if err := svc.HandleRunResult(context.Background(), fencingMeta(1), result); err != nil {
				t.Fatal(err)
			}
			next := repo.submissions[s.ID]
			if next.Status != domain.CodeSubmissionStatusQueued || next.Verdict != nil || next.ExecutionID == s.ExecutionID || next.Attempts != 2 {
				t.Fatalf("bad context accepted or not retried: %#v", next)
			}
			if err := svc.HandleRunResult(context.Background(), fencingMeta(1), result); err != nil {
				t.Fatal(err)
			}
			if repo.submissions[s.ID].Attempts != 2 {
				t.Fatal("duplicate consumed another attempt")
			}
			if err := svc.HandleRunResult(context.Background(), fencingMeta(2), fencingResult(next, "ok")); err != nil {
				t.Fatal(err)
			}
			got := repo.submissions[s.ID]
			if got.Verdict == nil || *got.Verdict != domain.ExecutionVerdictAccepted {
				t.Fatalf("valid retry failed: %#v", got)
			}
		})
	}
}

func TestResultWrongUserIsRejected(t *testing.T) {
	repo, svc, s := fencingSubmission(t)
	r := fencingResult(s, "ok")
	r.UserID = uuid.NewString()
	err := svc.HandleRunResult(context.Background(), fencingMeta(1), r)
	if !IsPermanentResultError(err) || repo.submissions[s.ID].Verdict != nil {
		t.Fatalf("wrong user result was not rejected: %v", err)
	}
}

// Вывод malicious fixtures sandbox не является доверенным отчётом checker.
func TestSolutionOutputCannotForgeAccepted(t *testing.T) {
	expected := []domain.TaskExample{{Input: "probe\n", Output: "TRUSTED_EXPECTED_42\n"}}
	for _, output := range []string{"harness overwritten\n", `{"status":"passed","tests_passed":100}`, "", "expected unavailable\n"} {
		result := execution.RunResult{Status: "ok", Summary: execution.ResultSummary{CasesTotal: 1}, Cases: []execution.CaseRunResult{{Index: 0, Status: "ok", Stdout: output}}}
		verdict, _, _ := verdictFromRunResult(result, expected)
		if verdict != domain.ExecutionVerdictWrongAnswer {
			t.Fatalf("untrusted output accepted: %s", verdict)
		}
	}
	result := execution.RunResult{Status: "invalid_request", Error: &execution.RunError{Code: "checker_error", Retryable: false}}
	verdict, _, _ := verdictFromRunResult(result, expected)
	if verdict != domain.ExecutionVerdictCheckerError {
		t.Fatalf("protocol error misclassified: %s", verdict)
	}
}

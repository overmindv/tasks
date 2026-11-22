package usecase

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/domain"
	"github.com/overmindv/tasks/internal/execution"
	"github.com/overmindv/tasks/internal/repository"
)

type editorialRepository struct {
	*codeSubmissionMemoryRepository
	suite *domain.PublishedSuite
}

func (r *editorialRepository) WithinTransaction(_ context.Context, fn func(repository.Repository) error) error {
	return fn(r)
}
func (r *editorialRepository) GetPublishedSuite(context.Context, uuid.UUID, uuid.UUID) (*domain.PublishedSuite, error) {
	return r.suite, nil
}

func TestEditorialExecution(t *testing.T) {
	t.Parallel()
	for _, output := range []string{"hello", "wrong"} {
		t.Run(output, func(t *testing.T) {
			t.Parallel()
			base, old, id, v := newCodeSubmissionService()
			base.task.Status = domain.TaskStatusDraft
			r := &editorialRepository{base, &domain.PublishedSuite{Reference: domain.SuiteReference{ID: uuid.New(), Version: "1", ChecksumSHA256: strings.Repeat("a", 64)}, Definition: domain.SuiteDefinition{Language: "python", LanguageVersion: "3.12", Checker: "exact", Limits: domain.SuiteLimits{CPUms: 1000, Wallms: 2000, MemoryMB: 64, PIDs: 32, StdoutBytes: 32768, StderrBytes: 32768, WorkspaceBytes: 1048576}, Cases: []domain.SuiteCase{{ID: "secret", Input: "hello", Output: "hello", Hidden: true}}}}}
			policy := old.policy
			policy.LearningOutcomes = true
			svc := NewCodeSubmissionService(r, policy)
			actor := uuid.New()
			input := CodeSubmissionInput{TaskVersionID: v, IdempotencyKey: uuid.New(), Language: "python", SourceCode: "print(input())", RunMode: domain.RunModeEditorial}
			if _, err := svc.Submit(context.Background(), id, actor, input); err == nil {
				t.Fatal("student selected editorial")
			}
			if _, err := svc.SubmitEditorial(context.Background(), id, actor, input, false); err == nil {
				t.Fatal("non-admin submitted editorial")
			}
			submitted, err := svc.SubmitEditorial(context.Background(), id, actor, input, true)
			if err != nil {
				t.Fatal(err)
			}
			again, err := svc.SubmitEditorial(context.Background(), id, actor, input, true)
			if err != nil || again.ID != submitted.ID || len(r.outbox) != 1 {
				t.Fatal("idempotency failed")
			}
			var req execution.RunRequest
			if err := json.Unmarshal(r.outbox[0].Payload, &req); err != nil {
				t.Fatal(err)
			}
			if req.Execution.Suite == nil || req.Execution.Suite.ID != r.suite.Reference.ID || req.Execution.Inputs != nil {
				t.Fatal("did not use immutable sandbox suite")
			}
			if _, err := svc.Get(context.Background(), submitted.ID, actor, false); err == nil {
				t.Fatal("reference leaked to former admin")
			}
			if _, err := svc.Get(context.Background(), submitted.ID, actor, true); err == nil {
				t.Fatal("editorial returned through public DTO")
			}
			if _, err := svc.GetEditorial(context.Background(), submitted.ID, actor, false); err == nil {
				t.Fatal("editorial read allowed without admin")
			}
			if safe, err := svc.GetEditorial(context.Background(), submitted.ID, actor, true); err != nil || safe.SourceCode != "" {
				t.Fatal("admin polling failed or leaked source")
			}
			result := execution.RunResult{SchemaVersion: execution.ResultSchemaVersion, SubmissionID: submitted.ID.String(), AttemptID: submitted.ExecutionID.String(), TaskID: id.String(), UserID: actor.String(), Status: "ok", CreatedAt: time.Now(), EvaluationContext: &execution.EvaluationContext{TaskVersionID: v.String(), SuiteSHA256: execution.InputSuiteSHA256([]string{"hello"})}, Summary: execution.ResultSummary{CasesTotal: 1}, Cases: []execution.CaseRunResult{{Index: 0, Status: "ok", Stdout: output, Stderr: "private"}}}
			metadata := ExecutionMessageMetadata{Topic: "results", Offset: 1, PayloadSHA256: strings.Repeat("a", 64)}
			if err := svc.HandleRunResult(context.Background(), metadata, result); err != nil {
				t.Fatal(err)
			}
			// При ошибочной эмиссии learning outcome nil-интерфейс fake завершит тест panic.
			completed := r.submissions[submitted.ID]
			want := domain.ExecutionVerdictAccepted
			if output == "wrong" {
				want = domain.ExecutionVerdictWrongAnswer
			}
			if completed.Verdict == nil || *completed.Verdict != want {
				t.Fatalf("verdict=%v", completed.Verdict)
			}
			if completed.Tests[0].Stdout != "" || completed.Tests[0].Stderr != "" {
				t.Fatal("hidden diagnostics retained")
			}
		})
	}
}

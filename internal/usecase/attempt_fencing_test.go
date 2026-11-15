package usecase

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/domain"
	"github.com/overmindv/tasks/internal/execution"
	"github.com/overmindv/tasks/internal/repository"
)

func fencingSubmission(t *testing.T) (*codeSubmissionMemoryRepository, *CodeSubmissionService, domain.CodeSubmission) {
	t.Helper()
	repo, svc, taskID, versionID := newCodeSubmissionService()
	submission, err := svc.Submit(context.Background(), taskID, uuid.New(), CodeSubmissionInput{
		TaskVersionID: versionID, IdempotencyKey: uuid.New(), Language: domain.ProgrammingLanguagePython, SourceFileName: "main.py", SourceCode: "print(input())",
	})
	if err != nil {
		t.Fatal(err)
	}
	return repo, svc, submission
}
func fencingResult(s domain.CodeSubmission, status string) execution.RunResult {
	r := execution.RunResult{SchemaVersion: execution.ResultSchemaVersion, SubmissionID: s.ID.String(), AttemptID: s.ExecutionID.String(), TaskID: s.TaskID.String(), UserID: s.UserID.String(), Status: status, CreatedAt: time.Now().UTC(), Cases: []execution.CaseRunResult{{Index: 0, Status: "ok", Stdout: "hello\n"}}}
	r.EvaluationContext = &execution.EvaluationContext{TaskVersionID: s.TaskVersionID.String(), SuiteSHA256: execution.InputSuiteSHA256([]string{"hello"})}
	r.Summary.CasesTotal = 1
	if status == "internal_error" {
		r.Summary.CasesTotal = 0
		r.Cases = nil
	}
	return r
}
func fencingMeta(offset int64) ExecutionMessageMetadata {
	return ExecutionMessageMetadata{Topic: "results", Offset: offset, PayloadSHA256: "fixture"}
}

func TestInitialAttemptHasFullDeadline(t *testing.T) {
	_, svc, s := fencingSubmission(t)
	if s.NextAttemptAt.Sub(s.CreatedAt) != svc.policy.RetryGrace {
		t.Fatalf("initial attempt deadline = %s, want %s", s.NextAttemptAt.Sub(s.CreatedAt), svc.policy.RetryGrace)
	}
}

func TestRetryRotatesAttemptAndFencesLateResult(t *testing.T) {
	repo, svc, first := fencingSubmission(t)
	// Старый код начинал с немедленно истёкшего срока; здесь проверяется именно retry.
	first.NextAttemptAt = time.Now().Add(time.Minute)
	repo.submissions[first.ID] = first
	if err := svc.HandleRunResult(context.Background(), fencingMeta(1), fencingResult(first, "internal_error")); err != nil {
		t.Fatal(err)
	}
	second := repo.submissions[first.ID]
	if second.ExecutionID == first.ExecutionID || second.Attempts != 2 {
		t.Fatalf("retry did not rotate attempt: first=%s next=%s attempts=%d", first.ExecutionID, second.ExecutionID, second.Attempts)
	}
	var request execution.RunRequest
	if err := json.Unmarshal(repo.outbox[1].Payload, &request); err != nil {
		t.Fatal(err)
	}
	if request.AttemptID != second.ExecutionID.String() {
		t.Fatal("outbox does not contain current attempt")
	}
	for _, offset := range []int64{2, 3} {
		if err := svc.HandleRunResult(context.Background(), fencingMeta(offset), fencingResult(first, "ok")); err != nil {
			t.Fatal(err)
		}
	}
	if got := repo.submissions[first.ID]; got.Status != domain.CodeSubmissionStatusQueued || got.ExecutionID != second.ExecutionID || got.Attempts != 2 {
		t.Fatalf("late result changed current attempt: %#v", got)
	}
	stale := 0
	for _, r := range repo.inbox {
		if r.Status == "stale" {
			stale++
		}
	}
	if stale != 2 {
		t.Fatalf("stale inbox count=%d want 2", stale)
	}
	if err := svc.HandleRunResult(context.Background(), fencingMeta(4), fencingResult(second, "ok")); err != nil {
		t.Fatal(err)
	}
	if got := repo.submissions[first.ID]; got.Verdict == nil || *got.Verdict != domain.ExecutionVerdictAccepted {
		t.Fatalf("current result rejected: %#v", got)
	}
}

func TestReaperDoesNotRetryRefreshedCandidate(t *testing.T) {
	repo, svc, first := fencingSubmission(t)
	first.NextAttemptAt = time.Now().Add(-time.Minute)
	repo.submissions[first.ID] = first
	if err := svc.requeueOrFailOne(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	second := repo.submissions[first.ID]
	if err := svc.requeueOrFailOne(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if got := repo.submissions[first.ID]; got.Attempts != 2 || got.ExecutionID != second.ExecutionID || len(repo.outbox) != 2 {
		t.Fatalf("stale candidate retried refreshed attempt: attempts=%d outbox=%d", got.Attempts, len(repo.outbox))
	}
}

func TestExpiredResultUsesSameTransitionAsReaper(t *testing.T) {
	for _, attempts := range []int{1, 3} {
		for _, byResult := range []bool{false, true} {
			repo, svc, s := fencingSubmission(t)
			s.Attempts = attempts
			s.NextAttemptAt = time.Now().Add(-time.Second)
			repo.submissions[s.ID] = s
			var err error
			if byResult {
				err = svc.HandleRunResult(context.Background(), fencingMeta(1), fencingResult(s, "ok"))
			} else {
				err = svc.requeueOrFailOne(context.Background(), s)
			}
			if err != nil {
				t.Fatal(err)
			}
			got := repo.submissions[s.ID]
			if attempts < svc.policy.MaxAttempts {
				if got.Attempts != 2 || got.ExecutionID == s.ExecutionID || got.Status != domain.CodeSubmissionStatusQueued {
					t.Fatalf("expired attempt not retried: result=%v state=%#v", byResult, got)
				}
			} else if got.Verdict == nil || *got.Verdict != domain.ExecutionVerdictInfrastructureError {
				t.Fatalf("expired final attempt: result=%v state=%#v", byResult, got)
			}
		}
	}
}

// Ошибка чтения БД должна ретраиться, а не превращаться в rejected result.
func TestResultReadFailureRemainsRetryable(t *testing.T) {
	for _, stage := range []string{"lock", "version"} {
		t.Run(stage, func(t *testing.T) {
			repo, svc, s := fencingSubmission(t)
			svc.repository = failedResultRead{Repository: repo, stage: stage}
			err := svc.HandleRunResult(context.Background(), fencingMeta(99), fencingResult(s, "ok"))
			if err == nil || IsPermanentResultError(err) {
				t.Fatalf("database read classified as permanent: %v", err)
			}
		})
	}
}

type failedResultRead struct {
	repository.Repository
	stage string
}

func (f failedResultRead) WithinTransaction(ctx context.Context, fn func(repository.Repository) error) error {
	return fn(f)
}
func (f failedResultRead) GetCodeSubmissionForUpdate(ctx context.Context, id uuid.UUID) (domain.CodeSubmission, error) {
	if f.stage == "lock" {
		return domain.CodeSubmission{}, errors.New("database unavailable")
	}
	return f.Repository.GetCodeSubmissionForUpdate(ctx, id)
}
func (f failedResultRead) GetTaskVersion(ctx context.Context, id, version uuid.UUID) (domain.TaskVersion, error) {
	if f.stage == "version" {
		return domain.TaskVersion{}, errors.New("database unavailable")
	}
	return f.Repository.GetTaskVersion(ctx, id, version)
}

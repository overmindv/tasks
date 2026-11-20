package usecase

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/domain"
	"github.com/overmindv/tasks/internal/execution"
)

func TestReplayRequiresAdminAndRotatesAttempt(t *testing.T) {
	repo, svc, first := fencingSubmission(t)
	if _, err := svc.Replay(context.Background(), first.ID, first.ExecutionID, uuid.New(), false); err == nil {
		t.Fatal("non-admin replay")
	}
	if _, err := svc.Replay(context.Background(), first.ID, first.ExecutionID, uuid.New(), true); err == nil {
		t.Fatal("active replay")
	}
	first.Status = domain.CodeSubmissionStatusCompleted
	v := domain.ExecutionVerdictInfrastructureError
	first.Verdict = &v
	repo.submissions[first.ID] = first
	next, err := svc.Replay(context.Background(), first.ID, first.ExecutionID, uuid.New(), true)
	if err != nil {
		t.Fatal(err)
	}
	if next.ExecutionID == first.ExecutionID || next.Status != domain.CodeSubmissionStatusQueued || next.Attempts != 1 {
		t.Fatalf("bad replay %+v", next)
	}
	if _, err = svc.Replay(context.Background(), first.ID, first.ExecutionID, uuid.New(), true); err == nil {
		t.Fatal("duplicate replay")
	}
	if err := svc.HandleRunResult(context.Background(), fencingMeta(99), fencingResult(first, "ok")); err != nil {
		t.Fatal(err)
	}
	if repo.submissions[first.ID].Status != domain.CodeSubmissionStatusQueued {
		t.Fatal("late result changed replay")
	}
	var request execution.RunRequest
	if err = json.Unmarshal(repo.outbox[len(repo.outbox)-1].Payload, &request); err != nil {
		t.Fatal(err)
	}
	if request.TraceID != first.CorrelationID.String() {
		t.Error("replay must keep the original correlation")
	}
	deadline, err := time.Parse(time.RFC3339Nano, request.Metadata["attempt_deadline"].(string))
	if err != nil || deadline.Sub(request.CreatedAt) != svc.policy.RetryGrace {
		t.Fatalf("deadline: %v", err)
	}
}

func (r *codeSubmissionMemoryRepository) ReplayCodeSubmission(_ context.Context, id, expected, next, actor uuid.UUID, at, deadline time.Time) (bool, error) {
	s := r.submissions[id]
	if s.ExecutionID != expected {
		return false, nil
	}
	s.ExecutionID = next
	s.Attempts = 1
	s.Status = domain.CodeSubmissionStatusQueued
	s.Verdict = nil
	s.NextAttemptAt = deadline
	r.submissions[id] = s
	return true, nil
}

func TestReplayRejectsStudentVerdicts(t *testing.T) {
	for _, verdict := range []domain.ExecutionVerdict{domain.ExecutionVerdictAccepted, domain.ExecutionVerdictWrongAnswer} {
		repo, svc, first := fencingSubmission(t)
		first.Status = domain.CodeSubmissionStatusCompleted
		first.Verdict = &verdict
		repo.submissions[first.ID] = first
		if _, err := svc.Replay(context.Background(), first.ID, first.ExecutionID, uuid.New(), true); err == nil {
			t.Fatalf("replayed %s", verdict)
		}
	}
}

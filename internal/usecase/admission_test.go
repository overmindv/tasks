package usecase

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/apperror"
	"github.com/overmindv/tasks/internal/domain"
)

func TestCodeAdmission(t *testing.T) {
	repo, svc, taskID, versionID := newCodeSubmissionService()
	svc.policy.MaxPendingPerUser = 1
	svc.policy.MaxPendingTotal = 2
	user := uuid.New()
	input := CodeSubmissionInput{TaskVersionID: versionID, IdempotencyKey: uuid.New(), Language: "python", SourceCode: "print(1)"}
	first, err := svc.Submit(context.Background(), taskID, user, input)
	if err != nil {
		t.Fatal(err)
	}
	again, err := svc.Submit(context.Background(), taskID, user, input)
	if err != nil || again.ID != first.ID {
		t.Fatal("retry consumed quota")
	}
	changed := input
	changed.IdempotencyKey = uuid.New()
	_, err = svc.Submit(context.Background(), taskID, user, changed)
	var public *apperror.Error
	if !errors.As(err, &public) || public.Code != apperror.RateLimited || public.RetryAfterSeconds != 15 {
		t.Fatalf("expected safe rate limit: %v", err)
	}
	if len(repo.outbox) != 1 {
		t.Fatal("rejected request enqueued")
	}
	first.Status = domain.CodeSubmissionStatusCompleted
	repo.submissions[first.ID] = first
	if _, err = svc.Submit(context.Background(), taskID, user, changed); err != nil {
		t.Fatal(err)
	}
}

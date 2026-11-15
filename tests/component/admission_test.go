//go:build component

package component

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	postgresadapter "github.com/overmindv/tasks/internal/adapter/postgres"
	"github.com/overmindv/tasks/internal/apperror"
	"github.com/overmindv/tasks/internal/domain"
	"github.com/overmindv/tasks/internal/repository"
	"github.com/overmindv/tasks/internal/usecase"
	"os"
	"sync"
	"testing"
	"time"
)

type failingAdmissionOutbox struct{ repository.Repository }

func (r failingAdmissionOutbox) WithinTransaction(ctx context.Context, fn func(repository.Repository) error) error {
	return r.Repository.WithinTransaction(ctx, func(tx repository.Repository) error { return fn(failingAdmissionOutbox{tx}) })
}
func (r failingAdmissionOutbox) InsertOutboxMessage(context.Context, domain.OutboxMessage) error {
	return errors.New("injected outbox failure")
}

func TestAdmissionPostgres(t *testing.T) {
	ctx := context.Background()
	dsn := os.Getenv("COMPONENT_TEST_DSN")
	if dsn == "" {
		t.Fatal("dedicated COMPONENT_TEST_DSN required")
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	repo := postgresadapter.New(pool)
	service := usecase.NewTaskService(repo)
	actor := uuid.New()
	var tasks []domain.TaskDetail
	for i := 0; i < 2; i++ {
		task, err := service.Create(ctx, domain.TaskInput{Title: "Admission", Statement: "Echo", TaskType: "programming", Difficulty: "easy", Examples: []domain.TaskExample{{Input: "hello", Output: "hello"}}}, actor)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = service.ChangeStatus(ctx, task.Task.ID, "published", actor); err != nil {
			t.Fatal(err)
		}
		tasks = append(tasks, task)
	}
	policy := usecase.CodeExecutionPolicy{RequestsTopic: "requests", RetryGrace: time.Minute, TimeLimit: time.Second, MemoryLimitBytes: 64 << 20, MaxPendingPerUser: 2, MaxPendingTotal: 100000}
	makeService := func(r repository.Repository, p usecase.CodeExecutionPolicy) *usecase.CodeSubmissionService {
		return usecase.NewCodeSubmissionService(r, p)
	}
	input := func(task domain.TaskDetail, key uuid.UUID) usecase.CodeSubmissionInput {
		return usecase.CodeSubmissionInput{TaskVersionID: task.Version.ID, IdempotencyKey: key, Language: "python", SourceCode: "print(input())"}
	}
	run := func(n int, fn func(int) (domain.CodeSubmission, error)) []domain.CodeSubmission {
		t.Helper()
		var wg sync.WaitGroup
		start := make(chan struct{})
		type outcome struct {
			s domain.CodeSubmission
			e error
		}
		out := make(chan outcome, n)
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) { defer wg.Done(); <-start; s, e := fn(i); out <- outcome{s, e} }(i)
		}
		close(start)
		wg.Wait()
		close(out)
		var accepted []domain.CodeSubmission
		for r := range out {
			if r.e == nil {
				accepted = append(accepted, r.s)
				continue
			}
			var public *apperror.Error
			if !errors.As(r.e, &public) || public.Code != apperror.RateLimited {
				t.Fatalf("unexpected admission failure: %v", r.e)
			}
		}
		return accepted
	}
	user := uuid.New()
	replicas := []*usecase.CodeSubmissionService{makeService(repo, policy), makeService(postgresadapter.New(pool), policy)}
	accepted := run(24, func(i int) (domain.CodeSubmission, error) {
		task := tasks[i%2]
		return replicas[i%2].Submit(ctx, task.Task.ID, user, input(task, uuid.New()))
	})
	if len(accepted) != 2 {
		t.Fatalf("per-user race admitted %d", len(accepted))
	}
	original := accepted[0]
	task := tasks[0]
	if task.Task.ID != original.TaskID {
		task = tasks[1]
	}
	retries := run(12, func(i int) (domain.CodeSubmission, error) {
		return replicas[i%2].Submit(ctx, task.Task.ID, user, input(task, original.IdempotencyKey))
	})
	for _, s := range retries {
		if s.ID != original.ID {
			t.Fatal("idempotent retry created row")
		}
	}
	if len(retries) != 12 {
		t.Fatal("idempotent retry rate limited")
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM code_submission_outbox WHERE aggregate_id IN (SELECT id FROM code_submissions WHERE user_id=$1)`, user).Scan(&count); err != nil || count != 2 {
		t.Fatalf("unexpected outbox count %d: %v", count, err)
	}
	verdict := domain.ExecutionVerdictInfrastructureError
	now := time.Now().UTC()
	original.Verdict = &verdict
	original.CompletedAt = &now
	if ok, err := repo.CompleteCodeSubmission(ctx, original); err != nil || !ok {
		t.Fatalf("terminal release: %v", err)
	}
	if _, err := replicas[0].Submit(ctx, task.Task.ID, user, input(task, uuid.New())); err != nil {
		t.Fatalf("released slot unavailable: %v", err)
	}
	// Global quota across distinct users and tasks; account for other fixture rows.
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM code_submissions WHERE status='queued'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	global := policy
	global.MaxPendingTotal = count + 3
	globalReplicas := []*usecase.CodeSubmissionService{makeService(repo, global), makeService(postgresadapter.New(pool), global)}
	accepted = run(24, func(i int) (domain.CodeSubmission, error) {
		task := tasks[i%2]
		return globalReplicas[i%2].Submit(ctx, task.Task.ID, uuid.New(), input(task, uuid.New()))
	})
	if len(accepted) != 3 {
		t.Fatalf("global race admitted %d", len(accepted))
	}
	// Two first requests sharing a key must return exactly one new submission.
	user = uuid.New()
	key := uuid.New()
	same := run(12, func(i int) (domain.CodeSubmission, error) {
		return replicas[i%2].Submit(ctx, tasks[0].Task.ID, user, input(tasks[0], key))
	})
	if len(same) != 12 {
		t.Fatal("same key refused")
	}
	for _, s := range same {
		if s.ID != same[0].ID {
			t.Fatal("duplicate submission")
		}
	}
	// A rolled-back outbox failure cannot reserve capacity.
	rollbackUser := uuid.New()
	rollbackInput := input(tasks[0], uuid.New())
	if _, err := makeService(failingAdmissionOutbox{repo}, policy).Submit(ctx, tasks[0].Task.ID, rollbackUser, rollbackInput); err == nil {
		t.Fatal("missing injected failure")
	}
	if existing, err := repo.FindCodeSubmissionByIdempotency(ctx, rollbackUser, rollbackInput.IdempotencyKey); err != nil || existing != nil {
		t.Fatalf("rollback retained row: %v", err)
	}
	if _, err := replicas[0].Submit(ctx, tasks[0].Task.ID, rollbackUser, rollbackInput); err != nil {
		t.Fatal(fmt.Errorf("retry after rollback: %w", err))
	}
}

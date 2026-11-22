//go:build component

package component

import (
	"context"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	postgresadapter "github.com/overmindv/tasks/internal/adapter/postgres"
	"github.com/overmindv/tasks/internal/domain"
	"github.com/overmindv/tasks/internal/usecase"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestReplayPostgresAtomicity(t *testing.T) {
	ctx := context.Background()
	dsn := os.Getenv("COMPONENT_TEST_DSN")
	if dsn == "" {
		t.Fatal("dedicated DB required")
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	repo := postgresadapter.New(pool)
	tasksvc := usecase.NewTaskService(repo)
	actor, user := uuid.New(), uuid.New()
	task, err := tasksvc.Create(ctx, domain.TaskInput{Title: "Replay", Statement: "Echo", TaskType: "programming", Difficulty: "easy", Examples: []domain.TaskExample{{Input: "hello", Output: "hello"}}}, actor)
	if err != nil {
		t.Fatal(err)
	}
	seedLegacyPublication(t, task.Task.ID, actor)
	policy := usecase.CodeExecutionPolicy{RequestsTopic: "requests", RetryGrace: time.Minute, TimeLimit: time.Second, MemoryLimitBytes: 64 << 20}
	svc := usecase.NewCodeSubmissionService(repo, policy)
	first, err := svc.Submit(ctx, task.Task.ID, user, usecase.CodeSubmissionInput{TaskVersionID: task.Version.ID, IdempotencyKey: uuid.New(), Language: "python", SourceCode: "print(input())"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE code_submissions SET status='completed',verdict='infrastructure_error',completed_at=now() WHERE id=$1`, first.ID); err != nil {
		t.Fatal(err)
	}
	fail := usecase.NewCodeSubmissionService(failingAdmissionOutbox{repo}, policy)
	if _, err = fail.Replay(ctx, first.ID, first.ExecutionID, actor, true); err == nil {
		t.Fatal("outbox error ignored")
	}
	var count int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM code_submission_replay_audit WHERE submission_id=$1`, first.ID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("audit escaped rollback: %d %v", count, err)
	}
	current, err := repo.GetCodeSubmission(ctx, first.ID)
	if err != nil || current.ExecutionID != first.ExecutionID || current.Status != domain.CodeSubmissionStatusCompleted {
		t.Fatalf("replay escaped rollback %v", err)
	}
	var wg sync.WaitGroup
	var success atomic.Int32
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := svc.Replay(ctx, first.ID, first.ExecutionID, actor, true); err == nil {
				success.Add(1)
			}
		}()
	}
	wg.Wait()
	if success.Load() != 1 {
		t.Fatalf("replays: %d", success.Load())
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM code_submission_replay_audit WHERE submission_id=$1 AND actor_id=$2 AND next_execution_id<>previous_execution_id`, first.ID, actor).Scan(&count); err != nil || count != 1 {
		t.Fatalf("audit %d %v", count, err)
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM code_submission_outbox WHERE aggregate_id=$1`, first.ID).Scan(&count); err != nil || count != 2 {
		t.Fatalf("outbox %d %v", count, err)
	}
}

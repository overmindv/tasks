//go:build component

package component

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	postgresadapter "github.com/overmindv/tasks/internal/adapter/postgres"
	"github.com/overmindv/tasks/internal/domain"
	"github.com/overmindv/tasks/internal/execution"
	"github.com/overmindv/tasks/internal/usecase"
	"os"
	"strings"
	"testing"
	"time"
)

func TestRunModesPostgres(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, os.Getenv("COMPONENT_TEST_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	repo := postgresadapter.New(pool)
	tasks := usecase.NewTaskService(repo)
	suites := usecase.NewSuiteService(repo, &suiteObjectStore{})
	actor, user := uuid.New(), uuid.New()
	task, err := tasks.Create(ctx, domain.TaskInput{Title: "Modes", Statement: "Echo", TaskType: "programming", Difficulty: "easy", Examples: []domain.TaskExample{{Input: "hello", Output: "hello"}}}, actor)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tasks.ChangeStatus(ctx, task.Task.ID, "published", actor); err != nil {
		t.Fatal(err)
	}
	svc := usecase.NewCodeSubmissionService(repo, usecase.CodeExecutionPolicy{RequestsTopic: "requests", RetryGrace: time.Minute, TimeLimit: time.Second, MemoryLimitBytes: 64 << 20})
	input := usecase.CodeSubmissionInput{TaskVersionID: task.Version.ID, IdempotencyKey: uuid.New(), Language: "python", SourceCode: "print(input())", RunMode: domain.RunModeGraded}
	if _, err := svc.Submit(ctx, task.Task.ID, user, input); err == nil {
		t.Fatal("graded without suite accepted")
	}
	input.RunMode = "invalid"
	if _, err := svc.Submit(ctx, task.Task.ID, user, input); err == nil {
		t.Fatal("invalid mode accepted")
	}
	input.RunMode = ""
	sample, err := svc.Submit(ctx, task.Task.ID, user, input)
	if err != nil || sample.RunMode != domain.RunModeSamples {
		t.Fatalf("default samples: %v", err)
	}
	input.RunMode = domain.RunModeGraded
	if _, err := svc.Submit(ctx, task.Task.ID, user, input); err == nil {
		t.Fatal("mode key conflict missing")
	}
	draft, err := suites.Create(ctx, task.Task.ID, task.Version.ID, suiteDefinition())
	if err != nil {
		t.Fatal(err)
	}
	published, err := suites.Publish(ctx, draft.ID, 1, actor)
	if err != nil {
		t.Fatal(err)
	}
	input.TaskVersionID = published.TaskVersionID
	input.IdempotencyKey = uuid.New()
	graded, err := svc.Submit(ctx, task.Task.ID, user, input)
	if err != nil {
		t.Fatal(err)
	}
	again, err := svc.Submit(ctx, task.Task.ID, user, input)
	if err != nil || again.ID != graded.ID {
		t.Fatal("graded retry not idempotent")
	}
	request := func(id uuid.UUID) execution.RunRequest {
		t.Helper()
		var raw []byte
		if err := pool.QueryRow(ctx, `SELECT payload FROM code_submission_outbox WHERE aggregate_id=$1 ORDER BY created_at DESC LIMIT 1`, id).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(raw), "PRIVATE_INPUT") {
			t.Fatal("private input in outbox")
		}
		var r execution.RunRequest
		if err := json.Unmarshal(raw, &r); err != nil {
			t.Fatal(err)
		}
		return r
	}
	r := request(graded.ID)
	if r.Execution.Suite == nil || *r.Execution.Suite != published.Reference || len(r.Execution.Inputs) != 0 {
		t.Fatal("missing pinned suite")
	}
	if r := request(sample.ID); r.Execution.Suite != nil || len(r.Execution.Inputs) != 1 {
		t.Fatal("samples not inline")
	}
	// Новая версия не меняет suite для повторной попытки старого решения.
	d2, err := suites.Create(ctx, task.Task.ID, published.TaskVersionID, suiteDefinition())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = suites.Publish(ctx, d2.ID, 1, actor); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE code_submissions SET next_attempt_at=now()-interval '1 second' WHERE id=$1`, graded.ID); err != nil {
		t.Fatal(err)
	}
	if err = svc.RequeueStaleCodeSubmissions(ctx); err != nil {
		t.Fatal(err)
	}
	retried, err := repo.GetCodeSubmission(ctx, graded.ID)
	if err != nil {
		t.Fatal(err)
	}
	r = request(graded.ID)
	if r.Execution.Suite == nil || *r.Execution.Suite != published.Reference || retried.ExecutionID == graded.ExecutionID {
		t.Fatal("retry lost immutable binding")
	}
	complete := func(s domain.CodeSubmission, inputs, outputs []string) {
		t.Helper()
		r := execution.RunResult{SchemaVersion: execution.ResultSchemaVersion, SubmissionID: s.ID.String(), AttemptID: s.ExecutionID.String(), TaskID: s.TaskID.String(), Status: "ok", CreatedAt: time.Now().UTC(), EvaluationContext: &execution.EvaluationContext{TaskVersionID: s.TaskVersionID.String(), SuiteSHA256: execution.InputSuiteSHA256(inputs)}}
		for i, o := range outputs {
			r.Cases = append(r.Cases, execution.CaseRunResult{Index: i, Status: "ok", Stdout: o, Stderr: o})
		}
		r.Summary.CasesTotal = len(r.Cases)
		if err := svc.HandleRunResult(ctx, usecase.ExecutionMessageMetadata{Topic: s.ID.String(), Offset: 1, PayloadSHA256: strings.Repeat("a", 64)}, r); err != nil {
			t.Fatal(err)
		}
	}
	complete(sample, []string{"hello"}, []string{"hello"})
	progress := usecase.NewProgressService(repo)
	before, err := progress.Get(ctx, user, user)
	if err != nil || before.Attempts != 0 || before.Score != 0 {
		t.Fatalf("sample affected progress: %+v %v", before, err)
	}
	complete(retried, []string{"hello", "PRIVATE_INPUT"}, []string{"hello", "PRIVATE_INPUT"})
	saved, err := repo.GetCodeSubmission(ctx, graded.ID)
	if err != nil || saved.Verdict == nil || *saved.Verdict != domain.ExecutionVerdictAccepted {
		t.Fatalf("graded result: %v", err)
	}
	if len(saved.Tests) != 2 || saved.Tests[1].Stdout != "" || saved.Tests[1].Stderr != "" || saved.Tests[1].SampleVisible {
		t.Fatal("private output persisted")
	}
	public, err := svc.Get(ctx, graded.ID, user, false)
	if err != nil || len(public.Tests) != 1 {
		t.Fatal("hidden case visible")
	}
	after, err := progress.Get(ctx, user, user)
	if err != nil || after.Attempts != 1 || after.Score != 10 {
		t.Fatalf("graded missing progress: %+v %v", after, err)
	}
}

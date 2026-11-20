//go:build component

package component

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	postgresadapter "github.com/overmindv/tasks/internal/adapter/postgres"
	"github.com/overmindv/tasks/internal/domain"
	"github.com/overmindv/tasks/internal/execution"
	"github.com/overmindv/tasks/internal/observability"
	"github.com/overmindv/tasks/internal/repository"
	"github.com/overmindv/tasks/internal/transport/httpapi"
	"github.com/overmindv/tasks/internal/usecase"
)

func TestAttemptFencingPostgres(t *testing.T) {
	ctx := context.Background()
	dsn := os.Getenv("COMPONENT_TEST_DSN")
	if dsn == "" {
		t.Fatal("COMPONENT_TEST_DSN is required (dedicated database only)")
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store := postgresadapter.New(pool)
	policy := usecase.CodeExecutionPolicy{RequestsTopic: "requests", MaxAttempts: 3, RetryGrace: time.Minute, TimeLimit: time.Second, MemoryLimitBytes: 64 << 20}
	svc := usecase.NewCodeSubmissionService(store, policy)
	mux := http.NewServeMux()
	httpapi.Register(mux, usecase.NewTaskService(store), usecase.NewSubmissionService(store), svc, usecase.NewCandidateService(store), usecase.NewProblemListService(store), slog.New(slog.NewTextHandler(io.Discard, nil)), "test-token")
	admin := uuid.NewString()
	task := componentTaskRequest(t, mux, http.MethodPost, "/v1/admin/tasks", admin, "admin", map[string]any{
		"title": "Attempt fencing", "statement": "Echo", "task_type": "programming", "difficulty": "easy", "options": []any{},
		"examples": []map[string]any{{"input": "hello", "output": "hello"}},
	}, http.StatusCreated)
	componentTaskRequest(t, mux, http.MethodPatch, "/v1/admin/tasks/"+task.ID+"/status", admin, "admin", map[string]string{"status": "published"}, http.StatusOK)
	create := func() domain.CodeSubmission {
		t.Helper()
		s, err := svc.Submit(ctx, uuid.MustParse(task.ID), uuid.New(), usecase.CodeSubmissionInput{TaskVersionID: uuid.MustParse(task.TaskVersionID), IdempotencyKey: uuid.New(), Language: domain.ProgrammingLanguagePython, SourceCode: "print(input())"})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	get := func(id uuid.UUID) domain.CodeSubmission {
		t.Helper()
		s, err := store.GetCodeSubmission(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	meta := func(s domain.CodeSubmission, n int64) usecase.ExecutionMessageMetadata {
		return usecase.ExecutionMessageMetadata{Topic: s.ID.String(), Offset: n, PayloadSHA256: strings.Repeat("a", 64)}
	}
	result := func(s domain.CodeSubmission, status string) execution.RunResult {
		r := execution.RunResult{SchemaVersion: execution.ResultSchemaVersion, SubmissionID: s.ID.String(), AttemptID: s.ExecutionID.String(), TaskID: s.TaskID.String(), Status: status, CreatedAt: time.Now().UTC()}
		r.EvaluationContext = &execution.EvaluationContext{TaskVersionID: s.TaskVersionID.String(), SuiteSHA256: execution.InputSuiteSHA256([]string{"hello"})}
		if status == "ok" {
			r.Summary.CasesTotal = 1
			r.Cases = []execution.CaseRunResult{{Index: 0, Status: "ok", Stdout: "hello"}}
		}
		return r
	}
	expire := func(s domain.CodeSubmission) {
		t.Helper()
		if _, err := pool.Exec(ctx, "UPDATE code_submissions SET next_attempt_at=now()-interval '1 second' WHERE id=$1", s.ID); err != nil {
			t.Fatal(err)
		}
	}
	countOutbox := func(s domain.CodeSubmission) int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM code_submission_outbox WHERE aggregate_id=$1", s.ID).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	together := func(actions ...func() error) {
		t.Helper()
		var wg sync.WaitGroup
		start := make(chan struct{})
		errs := make(chan error, len(actions))
		for _, action := range actions {
			wg.Add(1)
			go func(fn func() error) { defer wg.Done(); <-start; errs <- fn() }(action)
		}
		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	t.Run("stored sample visibility and legacy privacy", func(t *testing.T) {
		s := create()
		if err := svc.HandleRunResult(ctx, meta(s, 1), result(s, "ok")); err != nil {
			t.Fatal(err)
		}
		public := componentRawRequest(t, mux, http.MethodGet, "/v1/code-submissions/"+s.ID.String(), s.UserID.String(), "", nil, http.StatusOK)
		if !strings.Contains(public.Body.String(), "hello") || strings.Contains(public.Body.String(), "sample_visible") {
			t.Fatalf("sample output lost or internal marker leaked: %s", public.Body.String())
		}
		// Legacy/hidden данные не должны публиковаться даже при похожем на sample test_id.
		_, err := pool.Exec(ctx, `UPDATE code_submissions SET
   test_results='[{"test_id":"open-1","stdout":"HIDDEN_INPUT","stderr":"HIDDEN_INPUT"}]',
   execution_result='{"stdout":"HIDDEN_INPUT","stderr":"HIDDEN_INPUT","duration_ms":42}',
   error_code='HIDDEN_INPUT',error_message='https://artifact.invalid/HIDDEN_INPUT' WHERE id=$1`, s.ID)
		if err != nil {
			t.Fatal(err)
		}
		for _, route := range []struct{ path, user, role string }{
			{"/v1/code-submissions/" + s.ID.String(), s.UserID.String(), ""},
			{"/v1/code-submissions/" + s.ID.String(), admin, "admin"},
			{"/v1/me/code-submissions", s.UserID.String(), ""},
		} {
			response := componentRawRequest(t, mux, http.MethodGet, route.path, route.user, route.role, nil, http.StatusOK)
			if strings.Contains(response.Body.String(), "HIDDEN_INPUT") || !strings.Contains(response.Body.String(), `"tests":[]`) {
				t.Fatalf("legacy leak: %s", response.Body.String())
			}
		}
	})

	t.Run("invalid complete result retries atomically", func(t *testing.T) {
		for _, kind := range []string{"missing", "duplicate", "checksum", "run failure"} {
			first := create()
			r := result(first, "ok")
			switch kind {
			case "missing":
				r.Cases = nil
				r.Summary.CasesTotal = 0
			case "duplicate":
				r.Cases = append(r.Cases, r.Cases[0])
				r.Summary.CasesTotal = 2
			case "checksum":
				r.EvaluationContext.SuiteSHA256 = strings.Repeat("0", 64)
			case "run failure":
				r.Status = "internal_error"
			}
			if err := svc.HandleRunResult(ctx, meta(first, 1), r); err != nil {
				t.Fatal(err)
			}
			next := get(first.ID)
			if next.Attempts != 2 || next.Verdict != nil || next.ExecutionID == first.ExecutionID || countOutbox(first) != 2 {
				t.Fatalf("%s accepted or not retried", kind)
			}
			if err := svc.HandleRunResult(ctx, meta(first, 1), r); err != nil {
				t.Fatal(err)
			}
			if get(first.ID).Attempts != 2 || countOutbox(first) != 2 {
				t.Fatal("duplicate retry")
			}
			if err := svc.HandleRunResult(ctx, meta(next, 2), result(next, "ok")); err != nil {
				t.Fatal(err)
			}
			final := get(first.ID)
			if final.Verdict == nil || *final.Verdict != domain.ExecutionVerdictAccepted {
				t.Fatalf("%s did not recover", kind)
			}
		}
	})

	t.Run("concurrent reaper result and late delivery", func(t *testing.T) {
		for i := 0; i < 8; i++ {
			first := create()
			persisted := get(first.ID)
			if persisted.NextAttemptAt.Sub(persisted.CreatedAt) < 59*time.Second {
				t.Fatal("initial deadline not persisted")
			}
			expire(first)
			together(func() error { return svc.RequeueStaleCodeSubmissions(ctx) }, func() error { return svc.HandleRunResult(ctx, meta(first, 1), result(first, "ok")) })
			second := get(first.ID)
			if second.Attempts != 2 || second.ExecutionID == first.ExecutionID || second.Status != domain.CodeSubmissionStatusQueued || countOutbox(first) != 2 {
				t.Fatalf("race created wrong state: attempts=%d outbox=%d", second.Attempts, countOutbox(first))
			}
			var raw []byte
			if err := pool.QueryRow(ctx, "SELECT payload FROM code_submission_outbox WHERE aggregate_id=$1 ORDER BY created_at DESC LIMIT 1", first.ID).Scan(&raw); err != nil {
				t.Fatal(err)
			}
			var request execution.RunRequest
			if err := json.Unmarshal(raw, &request); err != nil {
				t.Fatal(err)
			}
			if request.AttemptID != second.ExecutionID.String() || request.Metadata["task_version_id"] != second.TaskVersionID.String() {
				t.Fatal("outbox identity mismatch")
			}
			if err := svc.HandleRunResult(ctx, meta(first, 2), result(first, "internal_error")); err != nil {
				t.Fatal(err)
			}
			together(func() error { return svc.HandleRunResult(ctx, meta(second, 3), result(second, "ok")) }, func() error { return svc.HandleRunResult(ctx, meta(second, 3), result(second, "ok")) })
			final := get(first.ID)
			if final.Verdict == nil || *final.Verdict != domain.ExecutionVerdictAccepted || final.Attempts != 2 {
				t.Fatalf("current result was not finalized: %#v", final)
			}
			var stale int
			if err := pool.QueryRow(ctx, "SELECT count(*) FROM code_execution_result_inbox WHERE topic=$1 AND status='stale'", first.ID.String()).Scan(&stale); err != nil {
				t.Fatal(err)
			}
			if stale != 2 {
				t.Fatalf("stale inbox=%d want2", stale)
			}
		}
	})
	t.Run("duplicate infrastructure result creates one retry", func(t *testing.T) {
		first := create()
		together(func() error { return svc.HandleRunResult(ctx, meta(first, 1), result(first, "internal_error")) }, func() error { return svc.HandleRunResult(ctx, meta(first, 2), result(first, "internal_error")) })
		if get(first.ID).Attempts != 2 || countOutbox(first) != 2 {
			t.Fatal("duplicate consumed extra attempt")
		}
	})
	t.Run("bounded mixed reaper and result retries", func(t *testing.T) {
		first := create()
		for i := 0; i < 4; i++ {
			current := get(first.ID)
			expire(current)
			together(func() error { return svc.RequeueStaleCodeSubmissions(ctx) }, func() error {
				return svc.HandleRunResult(ctx, meta(current, int64(i)), result(current, "internal_error"))
			})
		}
		final := get(first.ID)
		if final.Attempts != 3 || final.Verdict == nil || *final.Verdict != domain.ExecutionVerdictInfrastructureError || countOutbox(first) != 3 {
			t.Fatalf("retry budget not enforced: attempts=%d outbox=%d", final.Attempts, countOutbox(first))
		}
	})
	t.Run("outbox failure rolls back ID counter and inbox", func(t *testing.T) {
		first := create()
		broken := usecase.NewCodeSubmissionService(failingAttemptOutbox{store}, policy)
		if err := broken.HandleRunResult(ctx, meta(first, 1), result(first, "internal_error")); err == nil {
			t.Fatal("expected outbox failure")
		}
		current := get(first.ID)
		if current.ExecutionID != first.ExecutionID || current.Attempts != 1 || countOutbox(first) != 1 {
			t.Fatal("partial retry committed")
		}
		var n int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM code_execution_result_inbox WHERE topic=$1", first.ID.String()).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Fatal("inbox survived rollback")
		}
		if err := svc.HandleRunResult(ctx, meta(first, 1), result(first, "internal_error")); err != nil {
			t.Fatal(err)
		}
		if get(first.ID).Attempts != 2 {
			t.Fatal("retry after rollback did not recover")
		}
	})
	t.Run("metrics count committed transitions", func(t *testing.T) {
		metrics := observability.NewExecution()
		token := strings.Repeat("x", 32)
		handler, err := metrics.Handler(token)
		if err != nil {
			t.Fatal(err)
		}
		scrape := func() string {
			req := httptest.NewRequest(http.MethodGet, observability.ExecutionMetricsPath, nil)
			req.Header.Set("Authorization", "Bearer "+token)
			res := httptest.NewRecorder()
			handler.ServeHTTP(res, req)
			return res.Body.String()
		}
		observed := usecase.NewCodeSubmissionService(store, policy).WithExecutionMetrics(metrics)
		broken := usecase.NewCodeSubmissionService(rollbackAfterWrite{store}, policy).WithExecutionMetrics(metrics)
		input := usecase.CodeSubmissionInput{
			TaskVersionID: uuid.MustParse(task.TaskVersionID), IdempotencyKey: uuid.New(),
			Language: domain.ProgrammingLanguagePython, SourceCode: "print(input())",
		}
		actor, taskID := uuid.New(), uuid.MustParse(task.ID)
		if _, err := broken.Submit(ctx, taskID, actor, input); err == nil {
			t.Fatal("expected submission rollback")
		}
		first, err := observed.Submit(ctx, taskID, actor, input)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = observed.Submit(ctx, taskID, actor, input); err != nil {
			t.Fatal(err)
		}
		if err := broken.HandleRunResult(ctx, meta(first, 1), result(first, "ok")); err == nil {
			t.Fatal("expected result rollback")
		}
		if strings.Contains(scrape(), "tasks_execution_verdicts_total{") {
			t.Error("rolled back verdict counted")
		}
		handle := func() error { return observed.HandleRunResult(ctx, meta(first, 1), result(first, "ok")) }
		together(handle, handle)
		for range 2 {
			if err := observed.RejectResult(ctx, meta(first, 2), nil, execution.InboxErrorInvalidEvent); err != nil {
				t.Fatal(err)
			}
		}
		body := scrape()
		for _, want := range []string{
			`tasks_execution_attempts_total{kind="initial"} 1`,
			`tasks_execution_verdicts_total{run_mode="samples",verdict="accepted"} 1`,
			`tasks_execution_final_latency_seconds_count{run_mode="samples"} 1`,
			`tasks_execution_rejected_results_total{reason="invalid_event"} 1`,
		} {
			if !strings.Contains(body, want+"\n") {
				t.Errorf("missing sample: %s", want)
			}
		}
	})
	t.Run("queue ages and outbox correlation", func(t *testing.T) {
		first := create()
		if _, err := pool.Exec(ctx, "UPDATE code_submissions SET created_at=now()-interval '2 hours' WHERE id=$1", first.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, "UPDATE code_submission_outbox SET created_at=now()-interval '1 hour', available_at=now()-interval '1 day' WHERE aggregate_id=$1", first.ID); err != nil {
			t.Fatal(err)
		}
		outboxAge, resultAge, err := store.ExecutionQueueAges(ctx)
		if err != nil || outboxAge < 3600 || resultAge < 7200 {
			t.Errorf("outbox=%f result=%f err=%v", outboxAge, resultAge, err)
		}
		token := uuid.New()
		claimed, err := store.ClaimOutboxMessages(ctx, 1, token, time.Now().Add(time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		if len(claimed) != 1 || claimed[0].CorrelationID != first.CorrelationID {
			t.Fatal("outbox claim missing submission correlation")
		}
		if err := store.MarkOutboxPublished(ctx, claimed[0].ID, token); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("adapter CAS rejects stale attempt", func(t *testing.T) {
		first := create()
		next := uuid.New()
		ok, err := store.BumpCodeSubmissionAttempt(ctx, first.ID, first.ExecutionID, next, 3, time.Now().Add(time.Minute))
		if err != nil || !ok {
			t.Fatalf("initial CAS: %v %v", ok, err)
		}
		ok, err = store.BumpCodeSubmissionAttempt(ctx, first.ID, first.ExecutionID, uuid.New(), 3, time.Now().Add(time.Minute))
		if err != nil || ok {
			t.Fatalf("stale CAS: %v %v", ok, err)
		}
		verdict := domain.ExecutionVerdictAccepted
		now := time.Now().UTC()
		first.Verdict = &verdict
		first.CompletedAt = &now
		ok, err = store.CompleteCodeSubmission(ctx, first)
		if err != nil || ok {
			t.Fatalf("stale completion CAS: %v %v", ok, err)
		}
	})
}

type failingAttemptOutbox struct{ repository.Repository }

type rollbackAfterWrite struct{ repository.Repository }

func (r rollbackAfterWrite) WithinTransaction(ctx context.Context, fn func(repository.Repository) error) error {
	return r.Repository.WithinTransaction(ctx, func(tx repository.Repository) error {
		if err := fn(tx); err != nil {
			return err
		}
		return errors.New("injected rollback after successful write")
	})
}

func (f failingAttemptOutbox) WithinTransaction(ctx context.Context, fn func(repository.Repository) error) error {
	return f.Repository.WithinTransaction(ctx, func(tx repository.Repository) error { return fn(failingAttemptOutbox{tx}) })
}
func (f failingAttemptOutbox) InsertOutboxMessage(context.Context, domain.OutboxMessage) error {
	return errors.New("injected outbox failure")
}

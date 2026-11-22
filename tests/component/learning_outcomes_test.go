//go:build component

package component

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	postgresadapter "github.com/overmindv/tasks/internal/adapter/postgres"
	"github.com/overmindv/tasks/internal/domain"
	"github.com/overmindv/tasks/internal/execution"
	"github.com/overmindv/tasks/internal/repository"
	"github.com/overmindv/tasks/internal/usecase"
)

var errOutcomeRollback = errors.New("rollback after inserting learning outcome")

type outcomeRollback struct {
	repository.Repository
	aggregateID uuid.UUID
}

func (r *outcomeRollback) WithinTransaction(ctx context.Context, fn func(repository.Repository) error) error {
	return r.Repository.WithinTransaction(ctx, func(tx repository.Repository) error {
		nested := &outcomeRollback{Repository: tx}
		err := fn(nested)
		r.aggregateID = nested.aggregateID
		return err
	})
}

func (r *outcomeRollback) InsertLearningOutcome(ctx context.Context, event domain.LearningOutcome) error {
	r.aggregateID = event.AggregateID
	if err := r.Repository.InsertLearningOutcome(ctx, event); err != nil {
		return err
	}
	return errOutcomeRollback
}

func openComponentDB(t testing.TB) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(os.Getenv("COMPONENT_TEST_DSN"))
	if err != nil || !strings.HasSuffix(cfg.ConnConfig.Database, "_test") {
		t.Fatal("COMPONENT_TEST_DSN must point to a dedicated _test database")
	}
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func publishOutcomeFixture(t testing.TB, tasks *usecase.TaskService, author uuid.UUID, in domain.TaskInput) domain.TaskDetail {
	t.Helper()
	task, err := tasks.Create(t.Context(), in, author)
	if err != nil {
		t.Fatal(err)
	}
	task = seedLegacyPublication(t, task.Task.ID, author)
	return task
}

func outcomeCount(t *testing.T, pool *pgxpool.Pool, id uuid.UUID, want int) {
	t.Helper()
	var got int
	err := pool.QueryRow(t.Context(), "SELECT count(*) FROM learning_outcome_outbox WHERE aggregate_id=$1", id).Scan(&got)
	if err != nil || got != want {
		t.Fatal("unexpected outcome count:", id, got, want, err)
	}
}

func readOutcome(t *testing.T, pool *pgxpool.Pool, id uuid.UUID) domain.LearningOutcome {
	t.Helper()
	var raw string
	var eventID uuid.UUID
	var topic, key string
	err := pool.QueryRow(t.Context(), `
		SELECT id, topic, message_key, payload::text FROM learning_outcome_outbox WHERE aggregate_id=$1`, id).Scan(&eventID, &topic, &key, &raw)
	if err != nil {
		t.Fatal(err)
	}
	var event domain.LearningOutcome
	decode := json.NewDecoder(strings.NewReader(raw))
	decode.DisallowUnknownFields()
	if err := decode.Decode(&event); err != nil {
		t.Fatal("unexpected fields in the wire payload:", err)
	}
	if err := decode.Decode(new(any)); err != io.EOF || topic != domain.LearningOutcomeTopic ||
		key != id.String() || eventID == uuid.Nil || event.EventID != eventID || event.AggregateID != id ||
		event.Payload.SubmissionID != id || event.EventType != topic || event.SchemaVersion != 1 ||
		event.AggregateVersion != 1 || event.Payload.CorrectionVersion != 1 || event.Payload.TopicIDs == nil ||
		event.OccurredAt.IsZero() || !event.OccurredAt.Equal(event.Payload.GradedAt) {
		t.Fatal("invalid outcome envelope:", event)
	}
	return event
}

func TestLearningOutcomeChoiceTransaction(t *testing.T) {
	ctx := t.Context()
	pool := openComponentDB(t)
	store := postgresadapter.New(pool)
	tasks := usecase.NewTaskService(store)
	author, learner, topic, nextTopic := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	in := domain.TaskInput{TopicID: &topic, Title: "Choice", Statement: "Choose an answer", TaskType: domain.TaskTypeSingleChoice,
		Options: []domain.OptionInput{{Text: "Wrong"}, {Text: "Answer", IsCorrect: true}}}
	task := publishOutcomeFixture(t, tasks, author, in)
	pick := usecase.SubmitInput{TaskVersionID: task.Version.ID, IdempotencyKey: uuid.New(), SelectedOptionIDs: []uuid.UUID{task.Version.Options[0].ID}}
	failing := &outcomeRollback{Repository: store}
	if _, err := usecase.NewSubmissionServiceWithOutcomes(failing).Submit(ctx, task.Task.ID, learner, pick); !errors.Is(err, errOutcomeRollback) {
		t.Fatal("outbox failure did not abort choice submission:", err)
	}
	outcomeCount(t, pool, failing.aggregateID, 0)
	if row, err := store.FindSubmissionByIdempotency(ctx, learner, pick.IdempotencyKey); err != nil || row != nil {
		t.Fatal("choice submission survived rollback:", err)
	}

	choices := usecase.NewSubmissionServiceWithOutcomes(store)
	submit := func(actor uuid.UUID) domain.Submission {
		t.Helper()
		row, err := choices.Submit(ctx, task.Task.ID, actor, pick)
		if err != nil {
			t.Fatal(err)
		}
		return row
	}
	first := submit(learner)
	old := readOutcome(t, pool, first.ID)
	if old.Payload.LearnerID != learner || old.Payload.TaskID != task.Task.ID || old.Payload.TaskVersionID != task.Version.ID ||
		old.Payload.Verdict != domain.ExecutionVerdictWrongAnswer || !old.Payload.FirstIndependentAttempt ||
		!slices.Equal(old.Payload.TopicIDs, []uuid.UUID{topic}) || old.CorrelationID != nil {
		t.Fatal("unexpected choice outcome:", old)
	}
	if retry := submit(learner); retry.ID != first.ID {
		t.Fatal("retry changed submission ID")
	}
	outcomeCount(t, pool, first.ID, 1)
	if readOutcome(t, pool, first.ID).EventID != old.EventID {
		t.Fatal("retry changed event ID")
	}

	in.TopicID = &nextTopic
	next, err := tasks.Update(ctx, task.Task.ID, in, author)
	if err != nil {
		t.Fatal(err)
	}
	pick = usecase.SubmitInput{TaskVersionID: next.Version.ID, IdempotencyKey: uuid.New(), SelectedOptionIDs: []uuid.UUID{next.Version.Options[1].ID}}
	seedLegacyPublication(t, task.Task.ID, author)
	nextEvent := readOutcome(t, pool, submit(learner).ID)
	if nextEvent.Payload.FirstIndependentAttempt || nextEvent.Payload.TaskVersionID != next.Version.ID ||
		!slices.Equal(nextEvent.Payload.TopicIDs, []uuid.UUID{nextTopic}) || nextEvent.Payload.Verdict != domain.ExecutionVerdictAccepted {
		t.Fatal("new version lost its topic snapshot or counted as the first attempt:", nextEvent)
	}
	own := readOutcome(t, pool, submit(author).ID)
	if own.Payload.FirstIndependentAttempt {
		t.Fatal("task author counted as independent")
	}
}

func TestLearningOutcomeGradedCompletion(t *testing.T) {
	ctx := t.Context()
	pool := openComponentDB(t)
	store := postgresadapter.New(pool)
	author, learner, topic := uuid.New(), uuid.New(), uuid.New()
	task := publishOutcomeFixture(t, usecase.NewTaskService(store), author,
		domain.TaskInput{TopicID: &topic, Title: "Code", Statement: "Echo", TaskType: domain.TaskTypeProgramming,
			Examples: []domain.TaskExample{{Input: "hello", Output: "hello"}}})
	suites := usecase.NewSuiteService(store, &suiteObjectStore{})
	draft, err := suites.Create(ctx, task.Task.ID, task.Version.ID, suiteDefinition())
	if err != nil {
		t.Fatal(err)
	}
	published, err := suites.Publish(ctx, draft.ID, draft.Revision, author)
	if err != nil {
		t.Fatal(err)
	}
	policy := usecase.CodeExecutionPolicy{LearningOutcomes: true, RequestsTopic: "requests", MaxAttempts: 1, RetryGrace: time.Minute,
		TimeLimit: time.Second, MemoryLimitBytes: 64 << 20}
	seedLegacyPublication(t, task.Task.ID, author)
	code := usecase.NewCodeSubmissionService(store, policy)
	submit := func(mode domain.RunMode) domain.CodeSubmission {
		t.Helper()
		s, err := code.Submit(ctx, task.Task.ID, learner, usecase.CodeSubmissionInput{
			TaskVersionID: published.TaskVersionID, IdempotencyKey: uuid.New(), Language: "python", SourceCode: "print(input())", RunMode: mode,
		})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	result := func(s domain.CodeSubmission, status string) execution.RunResult {
		inputs := []string{"hello"}
		if s.RunMode == domain.RunModeGraded {
			inputs = append(inputs, "PRIVATE_INPUT")
		}
		r := execution.RunResult{SchemaVersion: execution.ResultSchemaVersion, SubmissionID: s.ID.String(), TaskID: s.TaskID.String(),
			AttemptID: s.ExecutionID.String(), Status: status, CreatedAt: time.Now().UTC(),
			EvaluationContext: &execution.EvaluationContext{TaskVersionID: s.TaskVersionID.String(), SuiteSHA256: execution.InputSuiteSHA256(inputs)}}
		if status == execution.ResultStatusOK {
			for i, input := range inputs {
				r.Cases = append(r.Cases, execution.CaseRunResult{Index: i, Status: status, Stdout: input, Stderr: input})
			}
			r.Summary.CasesTotal = len(r.Cases)
		}
		return r
	}
	meta := func(s domain.CodeSubmission, offset int64) usecase.ExecutionMessageMetadata {
		return usecase.ExecutionMessageMetadata{Topic: s.ID.String(), Offset: offset, PayloadSHA256: strings.Repeat("1", 64)}
	}
	sample := submit(domain.RunModeSamples)
	if err := code.HandleRunResult(ctx, meta(sample, 1), result(sample, execution.ResultStatusOK)); err != nil {
		t.Fatal(err)
	}
	outcomeCount(t, pool, sample.ID, 0)
	infra := submit(domain.RunModeGraded)
	if err := code.HandleRunResult(ctx, meta(infra, 1), result(infra, execution.ResultStatusInternalError)); err != nil {
		t.Fatal(err)
	}
	old, err := store.GetCodeSubmission(ctx, infra.ID)
	if err != nil || old.Verdict == nil || *old.Verdict != domain.ExecutionVerdictInfrastructureError {
		t.Fatal("infrastructure failure did not settle:", err)
	}
	outcomeCount(t, pool, infra.ID, 0)

	first := submit(domain.RunModeGraded)
	failing := usecase.NewCodeSubmissionService(&outcomeRollback{Repository: store}, policy)
	if err := failing.HandleRunResult(ctx, meta(first, 1), result(first, execution.ResultStatusOK)); !errors.Is(err, errOutcomeRollback) {
		t.Fatal("outbox failure did not abort completion:", err)
	}
	outcomeCount(t, pool, first.ID, 0)
	old, err = store.GetCodeSubmission(ctx, first.ID)
	if err != nil || old.Status != domain.CodeSubmissionStatusQueued {
		t.Fatal("grade survived rollback:", err)
	}

	second := submit(domain.RunModeGraded)
	var group sync.WaitGroup
	start := make(chan struct{})
	errorsFromRun := make(chan error, 2)
	for _, row := range []domain.CodeSubmission{first, second} {
		group.Go(func() {
			<-start
			errorsFromRun <- code.HandleRunResult(ctx, meta(row, 1), result(row, execution.ResultStatusOK))
		})
	}
	close(start)
	group.Wait()
	close(errorsFromRun)
	for err := range errorsFromRun {
		if err != nil {
			t.Fatal(err)
		}
	}
	firstCount := 0
	for _, row := range []domain.CodeSubmission{first, second} {
		for _, offset := range []int64{1, 2} {
			if err := code.HandleRunResult(ctx, meta(row, offset), result(row, execution.ResultStatusOK)); err != nil {
				t.Fatal(err)
			}
		}
		outcomeCount(t, pool, row.ID, 1)
		event := readOutcome(t, pool, row.ID)
		if event.Payload.FirstIndependentAttempt {
			firstCount++
		}
		if event.Payload.LearnerID != learner || event.Payload.TaskID != task.Task.ID || event.Payload.TaskVersionID != published.TaskVersionID ||
			event.Payload.Verdict != domain.ExecutionVerdictAccepted || event.CorrelationID == nil || *event.CorrelationID != row.CorrelationID ||
			!slices.Equal(event.Payload.TopicIDs, []uuid.UUID{topic}) {
			t.Fatal("unexpected code outcome:", event)
		}
	}
	if firstCount != 1 {
		t.Fatal("expected one first assessed outcome across concurrent completions:", firstCount)
	}
}

func BenchmarkLearningOutcomeChoice(b *testing.B) {
	ctx := b.Context()
	pool := openComponentDB(b)
	store := postgresadapter.New(pool)
	author, learner, topic := uuid.New(), uuid.New(), uuid.New()
	task := publishOutcomeFixture(b, usecase.NewTaskService(store), author,
		domain.TaskInput{TopicID: &topic, Title: "Choice", Statement: "Choose an answer", TaskType: domain.TaskTypeSingleChoice,
			Options: []domain.OptionInput{{Text: "Wrong"}, {Text: "Answer", IsCorrect: true}}})
	if _, err := pool.Exec(ctx, `
		INSERT INTO submissions (user_id, task_id, task_version_id, idempotency_key, request_hash, verdict)
		SELECT $1, $2, $3, gen_random_uuid(), 'bench', 'wrong_answer' FROM generate_series(1, 50000)`, learner, task.Task.ID, task.Version.ID); err != nil {
		b.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "ANALYZE submissions"); err != nil {
		b.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		enabled bool
	}{{"disabled", false}, {"enabled", true}} {
		b.Run(tc.name, func(b *testing.B) {
			svc := usecase.NewSubmissionService(store)
			if tc.enabled {
				svc = usecase.NewSubmissionServiceWithOutcomes(store)
			}
			input := usecase.SubmitInput{TaskVersionID: task.Version.ID, SelectedOptionIDs: []uuid.UUID{task.Version.Options[1].ID}}
			for b.Loop() {
				input.IdempotencyKey = uuid.New()
				if _, err := svc.Submit(b.Context(), task.Task.ID, learner, input); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

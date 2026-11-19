//go:build component

package component

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	postgresadapter "github.com/overmindv/tasks/internal/adapter/postgres"
	"github.com/overmindv/tasks/internal/domain"
	"github.com/overmindv/tasks/internal/transport/httpapi"
	"github.com/overmindv/tasks/internal/usecase"
)

type learningResource struct {
	EventID          uuid.UUID `json:"event_id"`
	AggregateID      uuid.UUID `json:"aggregate_id"`
	AggregateVersion int64     `json:"aggregate_version"`
	Tombstone        bool      `json:"tombstone"`
	Metadata         *struct {
		VersionID        uuid.UUID `json:"task_version_id"`
		EstimatedMinutes *int      `json:"estimated_minutes"`
		TopicWeights     []struct {
			TopicID uuid.UUID `json:"topic_id"`
			Weight  int       `json:"weight"`
		} `json:"topic_weights"`
		Languages []struct {
			ID      string `json:"id"`
			Version string `json:"version"`
		} `json:"languages"`
	} `json:"metadata"`
}

type learningResourcePage struct {
	Watermark  int64              `json:"watermark"`
	Through    int64              `json:"through"`
	More       bool               `json:"more"`
	NextCursor string             `json:"next_cursor"`
	Resources  []learningResource `json:"resources"`
	Changes    []learningResource `json:"changes"`
}

func TestTaskLearningResourceReplay(t *testing.T) {
	ctx := t.Context()
	pool := resourceTestPool(t)
	store := postgresadapter.New(pool)
	tasks := usecase.NewTaskService(store)
	actor, topic := uuid.New(), uuid.New()
	minutes := 22
	input := domain.TaskInput{TopicID: &topic, Title: "Resource for replay", Statement: "Statement must stay out",
		TaskType: domain.TaskTypeSingleChoice, EstimatedMinutes: &minutes,
		Options: []domain.OptionInput{{Text: "Open answer"}, {Text: "Grader must stay out", IsCorrect: true}}}
	a := createResourceTask(t, tasks, input, actor, true)
	b := createResourceTask(t, tasks, input, actor, true)
	withoutTopic := input
	withoutTopic.TopicID = nil
	missingTopic := createResourceTask(t, tasks, withoutTopic, actor, true)
	draft := createResourceTask(t, tasks, input, actor, false)

	handler := resourceTestHandler(t, store, nil)
	first := resourcePage(t, handler, "/snapshot?limit=1")
	if b.Version.EstimatedMinutes == nil || *b.Version.EstimatedMinutes != minutes || a.Version.EstimatedMinutes == nil {
		t.Fatal("estimate not written with task versions")
	}
	input.Title = "New version"
	input.EstimatedMinutes = nil
	a2, err := tasks.Update(ctx, a.Task.ID, input, actor)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tasks.ChangeStatus(ctx, b.Task.ID, domain.TaskStatusArchived, actor); err != nil {
		t.Fatal(err)
	}

	before := allResourcePages(t, handler, first)
	oldA, oldB := before[a.Task.ID], before[b.Task.ID]
	if oldA.Metadata == nil || oldA.Metadata.VersionID != a.Version.ID || oldB.Metadata == nil || oldB.Metadata.VersionID != b.Version.ID {
		t.Fatal("snapshot mixed versions across pages")
	}
	if len(oldA.Metadata.TopicWeights) != 1 || oldA.Metadata.TopicWeights[0].TopicID != topic ||
		oldA.Metadata.TopicWeights[0].Weight != 1 || len(oldA.Metadata.Languages) != 0 {
		t.Fatal("invalid choice resource metadata")
	}
	if _, exists := before[missingTopic.Task.ID]; exists {
		t.Fatal("published task without topic included")
	}
	if _, exists := before[draft.Task.ID]; exists {
		t.Fatal("unpublished task included")
	}

	after := strconv.FormatInt(first.Watermark, 10)
	r1 := resourcePage(t, handler, "/changes?limit=1&after="+after)
	if len(r1.Changes) != 1 || !r1.More || r1.Changes[0].Metadata == nil ||
		r1.Changes[0].Metadata.VersionID != a2.Version.ID || r1.Changes[0].Metadata.EstimatedMinutes == nil ||
		*r1.Changes[0].Metadata.EstimatedMinutes != minutes {
		t.Fatal("first journal page should carry exact new version and retained estimate:", r1)
	}
	pinned := "&watermark=" + strconv.FormatInt(r1.Watermark, 10)
	replayed := resourcePage(t, handler, "/changes?limit=1&after="+after+pinned)
	if len(replayed.Changes) != 1 || replayed.Changes[0].EventID != r1.Changes[0].EventID {
		t.Fatal("journal retry changed event identity")
	}
	r2 := resourcePage(t, handler, "/changes?limit=1&after="+strconv.FormatInt(r1.Through, 10)+pinned)
	if r2.More || r2.Through != r1.Watermark || len(r2.Changes) != 1 || r2.Changes[0].AggregateID != b.Task.ID ||
		!r2.Changes[0].Tombstone || r2.Changes[0].Metadata != nil || r2.Changes[0].AggregateVersion <= oldB.AggregateVersion {
		t.Fatal("expected versioned archive marker without metadata:", r2)
	}
	available := availableResourceKeys(t, handler, a, a2, b, missingTopic, draft)
	if len(available) != 1 || available[0].VersionID != a2.Version.ID {
		t.Fatal("only current available version expected:", available)
	}

	input.SetEstimatedMinutes = true
	a3, err := tasks.Update(ctx, a.Task.ID, input, actor)
	if err != nil || a3.Version.EstimatedMinutes != nil {
		t.Fatal("null estimate was not cleared:", err)
	}
	if err := tasks.Delete(ctx, a.Task.ID, actor); err != nil {
		t.Fatal(err)
	}
	r3 := resourcePage(t, handler, "/changes?after="+strconv.FormatInt(r2.Through, 10))
	if len(r3.Changes) != 2 || r3.Changes[0].Metadata == nil || r3.Changes[0].Metadata.EstimatedMinutes != nil ||
		!r3.Changes[1].Tombstone || r3.Changes[1].AggregateID != a.Task.ID {
		t.Fatal("missing estimate update or deletion marker:", r3)
	}
}

func TestTaskLearningResourceRequiresGradedRuntime(t *testing.T) {
	pool := resourceTestPool(t)
	store := postgresadapter.New(pool)
	tasks := usecase.NewTaskService(store)
	actor, topic := uuid.New(), uuid.New()
	input := domain.TaskInput{TopicID: &topic, Title: "Programming resource",
		Statement: "Write a program", TaskType: domain.TaskTypeProgramming}
	task := createResourceTask(t, tasks, input, actor, true)

	runtimes := map[domain.ProgrammingLanguage]string{domain.ProgrammingLanguagePython: "3.12"}
	handler := resourceTestHandler(t, store, runtimes)
	before := resourcePage(t, handler, "/snapshot")
	if got := availableResourceKeys(t, handler, task); len(got) != 0 {
		t.Fatal("programming task without grader was included")
	}
	suites := usecase.NewSuiteService(store, &suiteObjectStore{objects: make(map[string][]byte)}, runtimes)
	draft, err := suites.Create(t.Context(), task.Task.ID, task.Version.ID, suiteDefinition())
	if err != nil {
		t.Fatal(err)
	}
	published, err := suites.Publish(t.Context(), draft.ID, draft.Revision, actor)
	if err != nil {
		t.Fatal(err)
	}
	page := resourcePage(t, handler, "/changes?after="+strconv.FormatInt(before.Watermark, 10))
	if len(page.Changes) != 1 || page.Changes[0].Metadata == nil {
		t.Fatal("suite publish must create resource:", page)
	}
	meta := page.Changes[0].Metadata
	if meta.VersionID != published.TaskVersionID || len(meta.Languages) != 1 ||
		meta.Languages[0].ID != "python" || meta.Languages[0].Version != "3.12" {
		t.Fatal("wrong pinned runtime:", meta)
	}
	current := task
	current.Version.ID = published.TaskVersionID
	if got := availableResourceKeys(t, handler, task, current); len(got) != 1 || got[0].VersionID != current.Version.ID {
		t.Fatal("new graded version should be available:", got)
	}
	disabled := resourceTestHandler(t, store, map[domain.ProgrammingLanguage]string{domain.ProgrammingLanguagePython: "3.13"})
	if got := availableResourceKeys(t, disabled, current); len(got) != 0 {
		t.Fatal("unsupported runtime version returned:", got)
	}
	next, err := tasks.Update(t.Context(), task.Task.ID, input, actor)
	if err != nil {
		t.Fatal(err)
	}
	after := resourcePage(t, handler, "/changes?after="+strconv.FormatInt(page.Through, 10))
	if len(after.Changes) != 1 || !after.Changes[0].Tombstone || after.Changes[0].Metadata != nil {
		t.Fatal("replacing a graded version with an ungraded one must hide the resource:", after)
	}
	if got := availableResourceKeys(t, handler, current, next); len(got) != 0 {
		t.Fatal("current and historical ungradable choices must be unavailable:", got)
	}
}

func TestTaskLearningResourceCommitOrder(t *testing.T) {
	ctx := t.Context()
	pool := resourceTestPool(t)
	store := postgresadapter.New(pool)
	tasks := usecase.NewTaskService(store)
	actor, topic := uuid.New(), uuid.New()
	input := domain.TaskInput{TopicID: &topic, Title: "Publish with rollback",
		Statement: "Choose an answer", TaskType: domain.TaskTypeSingleChoice,
		Options: []domain.OptionInput{{Text: "Wrong"}, {Text: "Correct", IsCorrect: true}}}
	a := createResourceTask(t, tasks, input, actor, false)
	b := createResourceTask(t, tasks, input, actor, false)
	initial, err := store.ResourceWatermark(ctx)
	if err != nil {
		t.Fatal(err)
	}
	first, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = first.Rollback(ctx) }()
	const publish = "UPDATE tasks SET status = 'published' WHERE id = $1"
	if _, err := first.Exec(ctx, publish, a.Task.ID); err != nil {
		t.Fatal(err)
	}
	if visible, err := store.ResourceWatermark(ctx); err != nil || visible != initial {
		t.Fatal("uncommitted revision was visible:", visible, err)
	}

	second, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = second.Rollback(ctx) }()
	if _, err := second.Exec(ctx, "SET LOCAL lock_timeout = '100ms'"); err != nil {
		t.Fatal(err)
	}
	_, err = second.Exec(ctx, publish, b.Task.ID)
	var locked *pgconn.PgError
	if !errors.As(err, &locked) || locked.Code != "55P03" {
		t.Fatal("later publication should wait for the watermark lock:", err)
	}
	if err := second.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := first.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := tasks.ChangeStatus(ctx, b.Task.ID, domain.TaskStatusPublished, actor); err != nil {
		t.Fatal(err)
	}
	page, err := usecase.NewResourceService(store, nil).Changes(ctx, nil, initial, 10)
	if err != nil || len(page.Changes) != 1 || page.Through != initial+1 || page.Changes[0].TaskID != b.Task.ID {
		t.Fatal("rolled back publication leaked into the journal:", page, err)
	}
}

func TestTaskDiagnosticEligibility(t *testing.T) {
	ctx := t.Context()
	store := postgresadapter.New(resourceTestPool(t))
	tasks := usecase.NewTaskService(store)
	author, learner, peer, topic := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	input := domain.TaskInput{TopicID: &topic, Title: "Choice for diagnosis", Statement: "Choose an answer",
		TaskType: domain.TaskTypeSingleChoice,
		Options:  []domain.OptionInput{{Text: "Wrong"}, {Text: "Correct", IsCorrect: true}}}
	clean := createResourceTask(t, tasks, input, author, true)
	seen := createResourceTask(t, tasks, input, author, true)
	draft := createResourceTask(t, tasks, input, author, false)
	own := createResourceTask(t, tasks, input, learner, true)
	edited := createResourceTask(t, tasks, input, author, true)
	if _, err := tasks.Update(ctx, edited.Task.ID, input, learner); err != nil {
		t.Fatal(err)
	}
	edited, err := tasks.Update(ctx, edited.Task.ID, input, author)
	if err != nil {
		t.Fatal(err)
	}
	choice := usecase.NewSubmissionService(store)
	attempt, err := choice.Submit(ctx, seen.Task.ID, learner, usecase.SubmitInput{
		TaskVersionID: seen.Version.ID, IdempotencyKey: uuid.New(), SelectedOptionIDs: []uuid.UUID{seen.Version.Options[0].ID},
	})
	if err != nil || len(attempt.CorrectOptionIDs) == 0 {
		t.Fatal("expected a recorded choice attempt with the answer:", err)
	}
	next, err := tasks.Update(ctx, seen.Task.ID, input, author)
	if err != nil {
		t.Fatal(err)
	}

	program := createResourceTask(t, tasks, domain.TaskInput{TopicID: &topic, Title: "Code for diagnosis",
		Statement: "Echo", TaskType: domain.TaskTypeProgramming,
		Examples: []domain.TaskExample{{Input: "hello", Output: "hello"}}}, author, true)
	codes := usecase.NewCodeSubmissionService(store, usecase.CodeExecutionPolicy{
		RequestsTopic: "requests", RetryGrace: time.Minute, TimeLimit: time.Second, MemoryLimitBytes: 64 << 20,
	})
	if _, err := codes.Submit(ctx, program.Task.ID, learner, usecase.CodeSubmissionInput{
		TaskVersionID: program.Version.ID, IdempotencyKey: uuid.New(), Language: "python", SourceCode: "print(input())",
	}); err != nil {
		t.Fatal(err)
	}
	runtimes := map[domain.ProgrammingLanguage]string{domain.ProgrammingLanguagePython: "3.12"}
	suites := usecase.NewSuiteService(store, &suiteObjectStore{objects: make(map[string][]byte)}, runtimes)
	suite, err := suites.Create(ctx, program.Task.ID, program.Version.ID, suiteDefinition())
	if err != nil {
		t.Fatal(err)
	}
	published, err := suites.Publish(ctx, suite.ID, suite.Revision, author)
	if err != nil {
		t.Fatal(err)
	}
	program.Version.ID = published.TaskVersionID

	handler := resourceTestHandler(t, store, runtimes)
	keys := []availableResourceKey{{seen.Task.ID, seen.Version.ID}, {next.Task.ID, next.Version.ID},
		{own.Task.ID, own.Version.ID}, {edited.Task.ID, edited.Version.ID},
		{draft.Task.ID, draft.Version.ID}, {clean.Task.ID, clean.Version.ID}, {program.Task.ID, program.Version.ID}}
	inputJSON, err := json.Marshal(map[string][]availableResourceKey{"resources": keys})
	if err != nil {
		t.Fatal(err)
	}
	check := func(actor uuid.UUID, raw []byte, wantStatus int, want ...availableResourceKey) {
		t.Helper()
		req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/v1/internal/resources/diagnostic-eligibility", bytes.NewReader(raw))
		req.Header.Set("Authorization", "Bearer "+strings.Repeat("x", 32))
		req.Header.Set("X-User-ID", actor.String())
		res := httptest.NewRecorder()
		handler.ServeHTTP(res, req)
		if res.Code != wantStatus {
			t.Fatal("unexpected eligibility status:", res.Code, res.Body.String())
		}
		if wantStatus == http.StatusOK {
			var got struct {
				Resources []availableResourceKey `json:"resources"`
			}
			decoder := json.NewDecoder(res.Body)
			decoder.DisallowUnknownFields()
			if err := decoder.Decode(&got); err != nil || got.Resources == nil || !slices.Equal(got.Resources, want) {
				t.Fatal("incorrect eligible keys:", got.Resources, "want:", want, err)
			}
		}
	}
	check(learner, inputJSON, http.StatusOK, keys[5])
	check(peer, inputJSON, http.StatusOK, keys[1], keys[2], keys[3], keys[5], keys[6])
	check(learner, []byte(`{"resources":[{"task_id":"`+own.Task.ID.String()+`","task_version_id":"`+own.Version.ID.String()+`"}]}`), http.StatusOK)
	check(learner, []byte(`{"resources":[]}`), http.StatusBadRequest)
	for _, count := range []int{2, 101} {
		raw, err := json.Marshal(map[string][]availableResourceKey{"resources": slices.Repeat(keys[:1], count)})
		if err != nil {
			t.Fatal(err)
		}
		check(learner, raw, http.StatusBadRequest)
	}
}

func BenchmarkDiagnosticEligibility(b *testing.B) {
	ctx := b.Context()
	pool := resourceTestPool(b)
	if !strings.HasSuffix(pool.Config().ConnConfig.Database, "_test") {
		b.Skip("benchmark requires a dedicated database ending in _test")
	}
	store := postgresadapter.New(pool)
	tasks := usecase.NewTaskService(store)
	author, learner, topic := uuid.New(), uuid.New(), uuid.New()
	input := domain.TaskInput{TopicID: &topic, Title: "Choice for diagnosis", Statement: "Choose an answer",
		TaskType: domain.TaskTypeSingleChoice,
		Options:  []domain.OptionInput{{Text: "Wrong"}, {Text: "Correct", IsCorrect: true}}}
	keys := make([]domain.ResourceKey, 100)
	for i := range keys {
		task := createResourceTask(b, tasks, input, author, true)
		keys[i] = domain.ResourceKey{TaskID: task.Task.ID, VersionID: task.Version.ID}
	}
	const history = `
		INSERT INTO submissions (user_id, task_id, task_version_id, idempotency_key, request_hash, verdict)
		VALUES ($1, $2, $3, gen_random_uuid(), 'bench', 'wrong_answer')`
	batch := &pgx.Batch{}
	for _, key := range keys[:50] {
		batch.Queue(history, learner, key.TaskID, key.VersionID)
	}
	if err := pool.SendBatch(ctx, batch).Close(); err != nil {
		b.Fatal(err)
	}
	otherTask := createResourceTask(b, tasks, input, author, true)
	if _, err := pool.Exec(ctx, `
		INSERT INTO submissions (user_id, task_id, task_version_id, idempotency_key, request_hash, verdict)
		SELECT $1, $2, $3, gen_random_uuid(), 'bench', 'wrong_answer'
		FROM generate_series(1, 100000)`, learner, otherTask.Task.ID, otherTask.Version.ID); err != nil {
		b.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "ANALYZE submissions"); err != nil {
		b.Fatal(err)
	}
	service := usecase.NewResourceService(store, nil)
	b.Run("availability", func(b *testing.B) {
		for b.Loop() {
			got, err := service.Available(b.Context(), keys)
			if err != nil || len(got) != 100 {
				b.Fatal("expected 100 available tasks:", len(got), err)
			}
		}
	})
	b.Run("diagnostic", func(b *testing.B) {
		for b.Loop() {
			got, err := service.DiagnosticEligible(b.Context(), learner, keys)
			if err != nil || len(got) != 50 {
				b.Fatal("expected 50 unseen tasks:", len(got), err)
			}
		}
	})
}

func resourceTestPool(t testing.TB) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("COMPONENT_TEST_DSN")
	if dsn == "" {
		t.Fatal("COMPONENT_TEST_DSN is required")
	}
	pool, err := pgxpool.New(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func createResourceTask(t testing.TB, svc *usecase.TaskService, input domain.TaskInput, actor uuid.UUID, publish bool) domain.TaskDetail {
	t.Helper()
	task, err := svc.Create(t.Context(), input, actor)
	if err != nil {
		t.Fatal(err)
	}
	if publish {
		task, err = svc.ChangeStatus(t.Context(), task.Task.ID, domain.TaskStatusPublished, actor)
		if err != nil {
			t.Fatal(err)
		}
	}
	return task
}

func resourceTestHandler(t *testing.T, store *postgresadapter.Postgres, versions map[domain.ProgrammingLanguage]string) http.Handler {
	t.Helper()
	mux := http.NewServeMux()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := httpapi.RegisterResources(mux, usecase.NewResourceService(store, versions), strings.Repeat("x", 32), logger); err != nil {
		t.Fatal(err)
	}
	return mux
}

func resourcePage(t *testing.T, handler http.Handler, path string) learningResourcePage {
	t.Helper()
	var page learningResourcePage
	resourceRequest(t, handler, http.MethodGet, path, nil, &page)
	return page
}

func allResourcePages(t *testing.T, handler http.Handler, page learningResourcePage) map[uuid.UUID]learningResource {
	t.Helper()
	index := make(map[uuid.UUID]learningResource)
	watermark := page.Watermark
	for {
		for _, row := range page.Resources {
			index[row.AggregateID] = row
		}
		if page.NextCursor == "" {
			return index
		}
		page = resourcePage(t, handler, "/snapshot?limit=100&watermark="+strconv.FormatInt(watermark, 10)+"&cursor="+page.NextCursor)
		if page.Watermark != watermark {
			t.Fatal("watermark changed during pagination")
		}
	}
}

type availableResourceKey struct {
	TaskID    uuid.UUID `json:"task_id"`
	VersionID uuid.UUID `json:"task_version_id"`
}

func availableResourceKeys(t *testing.T, handler http.Handler, tasks ...domain.TaskDetail) []availableResourceKey {
	t.Helper()
	keys := make([]availableResourceKey, 0, len(tasks))
	for _, task := range tasks {
		keys = append(keys, availableResourceKey{TaskID: task.Task.ID, VersionID: task.Version.ID})
	}
	var response struct {
		Resources []availableResourceKey `json:"resources"`
	}
	input, err := json.Marshal(map[string][]availableResourceKey{"resources": keys})
	if err != nil {
		t.Fatal(err)
	}
	resourceRequest(t, handler, http.MethodPost, "/availability", input, &response)
	return response.Resources
}

func resourceRequest(t *testing.T, handler http.Handler, method, path string, body []byte, output any) {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), method, "/v1/internal/resources"+path, bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+strings.Repeat("x", 32))
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatal("resource request failed:", res.Code, res.Body.String())
	}
	for _, private := range []string{`"is_correct"`, `"statement"`, `"definition"`, `"cases"`, `"manifest_sha256"`, "Grader must stay out", "PRIVATE_INPUT"} {
		if bytes.Contains(res.Body.Bytes(), []byte(private)) {
			t.Fatal("private resource field in response:", private)
		}
	}
	if err := json.Unmarshal(res.Body.Bytes(), output); err != nil {
		t.Fatal(err)
	}
}

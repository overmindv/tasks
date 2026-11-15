//go:build component

package component

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	postgresadapter "github.com/overmindv/tasks/internal/adapter/postgres"
	"github.com/overmindv/tasks/internal/domain"
	"github.com/overmindv/tasks/internal/usecase"
)

func suiteDefinition() domain.SuiteDefinition {
	return domain.SuiteDefinition{Language: "python", LanguageVersion: "3.12", Checker: "trim_newlines", Limits: domain.SuiteLimits{CPUms: 1000, Wallms: 2000, MemoryMB: 64, PIDs: 32, StdoutBytes: 32768, StderrBytes: 32768, WorkspaceBytes: 1048576}, Cases: []domain.SuiteCase{{ID: "public", Input: "hello", Output: "hello"}, {ID: "hidden", Input: "PRIVATE_INPUT", Output: "PRIVATE_INPUT", Hidden: true}}}
}
func TestSuiteVersionsPostgres(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, os.Getenv("COMPONENT_TEST_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store := postgresadapter.New(pool)
	svc := usecase.NewTaskService(store)
	actor := uuid.New()
	input := domain.TaskInput{Title: "Suite", Statement: "Echo", TaskType: "programming", Difficulty: "easy", Examples: []domain.TaskExample{{Input: "hello", Output: "hello"}}}
	first, err := svc.Create(ctx, input, actor)
	if err != nil {
		t.Fatal(err)
	}
	def := suiteDefinition()
	ref := domain.SuiteReference{ID: uuid.New(), Version: "1"}
	_, ref.ChecksumSHA256 = def.Manifest(ref.ID, ref.Version)
	if err := store.InsertPublishedSuite(ctx, domain.PublishedSuite{Reference: ref, TaskID: first.Task.ID, TaskVersionID: first.Version.ID, Definition: def}); err != nil {
		t.Fatal(err)
	}
	second, err := svc.Update(ctx, first.Task.ID, input, actor)
	if err != nil {
		t.Fatal(err)
	}
	def.Limits.CPUms = 1100
	ref.Version = "2"
	_, ref.ChecksumSHA256 = def.Manifest(ref.ID, ref.Version)
	if err := store.InsertPublishedSuite(ctx, domain.PublishedSuite{Reference: ref, TaskID: first.Task.ID, TaskVersionID: second.Version.ID, Definition: def}); err != nil {
		t.Fatal(err)
	}
	old, err := store.GetPublishedSuite(ctx, first.Task.ID, first.Version.ID)
	if err != nil || old.Definition.Limits.CPUms != 1000 {
		t.Fatalf("old suite changed: %v", err)
	}
	newer, err := store.GetPublishedSuite(ctx, first.Task.ID, second.Version.ID)
	if err != nil || newer.Definition.Limits.CPUms != 1100 {
		t.Fatalf("new suite not stored: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE task_version_suites SET suite_version='3' WHERE task_version_id=$1`, first.Version.ID); err == nil {
		t.Fatal("published suite mutable")
	}
}

type suiteObjectStore struct {
	fail    bool
	objects map[string][]byte
}

func (s *suiteObjectStore) PutImmutable(_ context.Context, key string, payload []byte) error {
	if s.fail {
		return fmt.Errorf("storage unavailable")
	}
	if s.objects == nil {
		s.objects = map[string][]byte{}
	}
	if old, ok := s.objects[key]; ok && !bytes.Equal(old, payload) {
		return fmt.Errorf("immutable object conflict")
	}
	s.objects[key] = append([]byte{}, payload...)
	return nil
}
func TestSuitePublishingPostgres(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, os.Getenv("COMPONENT_TEST_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store := postgresadapter.New(pool)
	tasks := usecase.NewTaskService(store)
	objects := &suiteObjectStore{}
	svc := usecase.NewSuiteService(store, objects)
	actor := uuid.New()
	task, err := tasks.Create(ctx, domain.TaskInput{Title: "Publish", Statement: "Echo", TaskType: "programming", Difficulty: "easy", Examples: []domain.TaskExample{{Input: "hello", Output: "hello"}}}, actor)
	if err != nil {
		t.Fatal(err)
	}
	definition := suiteDefinition()
	definition.Cases[1].Output = "EXPECTED_ONLY_TASKS"
	draft, err := svc.Create(ctx, task.Task.ID, task.Version.ID, definition)
	if err != nil {
		t.Fatal(err)
	}
	objects.fail = true
	if _, err := svc.Publish(ctx, draft.ID, 1, actor); err == nil {
		t.Fatal("storage failure published")
	}
	unchanged, err := tasks.GetAdmin(ctx, task.Task.ID)
	if err != nil || unchanged.Version.ID != task.Version.ID {
		t.Fatal("upload failure changed version")
	}
	objects.fail = false
	published, err := svc.Publish(ctx, draft.ID, 1, actor)
	if err != nil {
		t.Fatal(err)
	}
	if published.TaskVersionID == task.Version.ID {
		t.Fatal("publish did not create version")
	}
	again, err := svc.Publish(ctx, draft.ID, 1, actor)
	if err != nil || again.TaskVersionID != published.TaskVersionID {
		t.Fatal("repeat publish not idempotent")
	}
	for _, raw := range objects.objects {
		if bytes.Contains(raw, []byte("EXPECTED_ONLY_TASKS")) {
			t.Fatal("expected output in manifest")
		}
	}
	if _, err := svc.Update(ctx, draft.ID, 1, definition); err == nil {
		t.Fatal("published draft editable")
	}
	if err := svc.Delete(ctx, draft.ID, 1); err == nil {
		t.Fatal("published draft deletable")
	}
	second, err := svc.Create(ctx, task.Task.ID, published.TaskVersionID, definition)
	if err != nil {
		t.Fatal(err)
	}
	definition.Limits.CPUms = 1200
	second, err = svc.Update(ctx, second.ID, 1, definition)
	if err != nil || second.Revision != 2 {
		t.Fatal("draft revision not updated")
	}
	if _, err := svc.Publish(ctx, second.ID, 1, actor); err == nil {
		t.Fatal("stale revision published")
	}
	newer, err := svc.Publish(ctx, second.ID, 2, actor)
	if err != nil {
		t.Fatal(err)
	}
	old, err := store.GetPublishedSuite(ctx, task.Task.ID, published.TaskVersionID)
	if err != nil || old.Definition.Limits.CPUms != 1000 || newer.Definition.Limits.CPUms != 1200 {
		t.Fatal("historical suite changed")
	}
}

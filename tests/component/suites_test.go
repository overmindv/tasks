//go:build component

package component

import (
	"context"
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

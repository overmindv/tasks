//go:build component

package component

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	postgresadapter "github.com/overmindv/tasks/internal/adapter/postgres"
	"github.com/overmindv/tasks/internal/domain"
	"github.com/overmindv/tasks/internal/execution"
	"github.com/overmindv/tasks/internal/usecase"
)

type activeTopic struct{}

func (activeTopic) ValidateTopic(context.Context, uuid.UUID) error { return nil }

func TestEditorialPublicationPostgres(t *testing.T) {
	ctx := t.Context()
	pool := openComponentDB(t)
	repo := postgresadapter.New(pool)
	versions := map[domain.ProgrammingLanguage]string{"python": "3.12"}
	tasks := usecase.NewTaskService(repo).WithPublicationChecks(activeTopic{}, versions)
	actor, topic := uuid.New(), uuid.New()
	task, err := tasks.Create(ctx, domain.TaskInput{Title: "Editorial echo", Statement: "Echo input", TaskType: domain.TaskTypeProgramming, Difficulty: domain.DifficultyEasy, TopicID: &topic, Source: &domain.TaskSource{SourceID: "author", SourceName: "Author", SourceURL: "https://example.org/task"}}, actor)
	if err != nil {
		t.Fatal(err)
	}
	publish := func(want bool) {
		t.Helper()
		_, e := tasks.ChangeStatus(ctx, task.Task.ID, domain.TaskStatusPublished, actor)
		if (e == nil) != want {
			t.Fatalf("publication want %v: %v", want, e)
		}
	}
	publish(false)
	suites := usecase.NewSuiteService(repo, &suiteObjectStore{})
	draft, err := suites.Create(ctx, task.Task.ID, task.Version.ID, suiteDefinition())
	if err != nil {
		t.Fatal(err)
	}
	suite, err := suites.Publish(ctx, draft.ID, 1, actor)
	if err != nil {
		t.Fatal(err)
	}
	publish(false)
	code := usecase.NewCodeSubmissionService(repo, usecase.CodeExecutionPolicy{RequestsTopic: "editorial-test", RetryGrace: time.Minute, LanguageVersions: versions, LearningOutcomes: true})
	run := func(output string, offset int64) {
		t.Helper()
		submission, e := code.SubmitEditorial(ctx, task.Task.ID, actor, usecase.CodeSubmissionInput{TaskVersionID: suite.TaskVersionID, IdempotencyKey: uuid.New(), Language: "python", SourceCode: "print(input())"}, true)
		if e != nil {
			t.Fatal(e)
		}
		publish(false)
		result := execution.RunResult{SchemaVersion: execution.ResultSchemaVersion, SubmissionID: submission.ID.String(), AttemptID: submission.ExecutionID.String(), TaskID: task.Task.ID.String(), UserID: actor.String(), Status: "ok", CreatedAt: time.Now(), EvaluationContext: &execution.EvaluationContext{TaskVersionID: suite.TaskVersionID.String(), SuiteSHA256: execution.InputSuiteSHA256([]string{"hello", "PRIVATE_INPUT"})}, Summary: execution.ResultSummary{CasesTotal: 2}, Cases: []execution.CaseRunResult{{Index: 0, Status: "ok", Stdout: "hello"}, {Index: 1, Status: "ok", Stdout: output}}}
		e = code.HandleRunResult(ctx, usecase.ExecutionMessageMetadata{Topic: task.Task.ID.String(), Offset: offset, PayloadSHA256: strings.Repeat("a", 64)}, result)
		if e != nil {
			t.Fatal(e)
		}
		outcomeCount(t, pool, submission.ID, 0)
		mine, e := code.ListMine(ctx, domain.CodeSubmissionFilter{UserID: actor, Limit: 10})
		if e != nil || len(mine) != 0 {
			t.Fatalf("editorial leaked into history: %v %v", mine, e)
		}
	}
	run("wrong", 1)
	publish(false)
	run("PRIVATE_INPUT", 2)
	// Часы реплик могут расходиться: старый accepted не должен обойти новый run.
	if _, err := pool.Exec(ctx, `UPDATE code_submissions SET created_at=created_at+interval '1 day' WHERE task_version_id=$1 AND verdict='accepted'`, suite.TaskVersionID); err != nil {
		t.Fatal(err)
	}
	run("wrong", 3)
	publish(false)
	run("PRIVATE_INPUT", 4)
	publish(true)
	updated, err := tasks.Update(ctx, task.Task.ID, domain.TaskInput{Title: "Changed", Statement: "New statement", TaskType: domain.TaskTypeProgramming, Difficulty: domain.DifficultyEasy, TopicID: &topic}, actor)
	if err != nil || updated.Task.Status != domain.TaskStatusDraft {
		t.Fatalf("update did not reset publication: %v", err)
	}
	publish(false)
	passed, err := repo.EditorialCheckPassed(ctx, task.Task.ID, suite.TaskVersionID)
	if err != nil || !passed {
		t.Fatal("historical approval lost")
	}
}

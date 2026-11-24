package usecase

import (
	"context"
	"encoding/json"
	"testing"

	"strings"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/domain"
	"github.com/overmindv/tasks/internal/execution"
)

const (
	sqliteVersion     = "3.46"
	sqliteFileName    = "solution.sql"
	sqliteInvalidJSON = "not json"
)

const sqliteCaseFixture = `{"ordered":false,"tables":[{"name":"users","columns":[{"name":"id","type":"INTEGER"},{"name":"name","type":"TEXT"}],"rows":[[1,"ann"],[2,null]]}]}`

func sqliteSuite(ordered bool, expected string, hidden bool) *domain.PublishedSuite {
	fixture := sqliteCaseFixture
	if ordered {
		fixture = `{"ordered":true,"tables":[{"name":"users","columns":[{"name":"id","type":"INTEGER"}],"rows":[[1],[2]]}]}`
	}

	return &domain.PublishedSuite{
		Reference: domain.SuiteReference{ID: uuid.New(), Version: "1", ChecksumSHA256: strings.Repeat("a", 64)},
		Definition: domain.SuiteDefinition{
			Language: domain.ProgrammingLanguageSQLite, LanguageVersion: sqliteVersion, Checker: domain.SQLiteChecker,
			Limits: domain.SuiteLimits{CPUms: 1000, Wallms: 2000, MemoryMB: 64, PIDs: 32, StdoutBytes: 32768, StderrBytes: 32768, WorkspaceBytes: 1048576},
			Cases:  []domain.SuiteCase{{ID: "case-1", Input: fixture, Output: expected, Hidden: hidden}},
		},
	}
}

// TestSQLiteGradedComparison проверяет сравнение строк с дублями и настройкой порядка.
func TestSQLiteGradedComparison(t *testing.T) {
	t.Parallel()

	run := func(stdout string) execution.RunResult {
		return execution.RunResult{Status: "ok", Summary: execution.ResultSummary{CasesTotal: 1}, Cases: []execution.CaseRunResult{{Index: 0, Status: "ok", Stdout: stdout}}}
	}
	unordered := sqliteSuite(false, `[[2,null],[1,"ann"]]`, true)
	verdict, tests, _ := verdictFromSuiteResult(run(`[[1,"ann"],[2,null]]`), unordered)
	if verdict != domain.ExecutionVerdictAccepted || len(tests) != 1 || tests[0].Stdout != "" || tests[0].SampleVisible {
		t.Fatalf("unordered verdict=%s tests=%+v", verdict, tests)
	}
	verdict, _, _ = verdictFromSuiteResult(run(`[[1,"ann"]]`), unordered)
	if verdict != domain.ExecutionVerdictWrongAnswer {
		t.Fatalf("missing row verdict=%s", verdict)
	}
	ordered := sqliteSuite(true, `[[1],[2]]`, true)
	verdict, _, _ = verdictFromSuiteResult(run(`[[2],[1]]`), ordered)
	if verdict != domain.ExecutionVerdictWrongAnswer {
		t.Fatalf("ordered mismatch verdict=%s", verdict)
	}
	verdict, _, _ = verdictFromSuiteResult(run(`[[1],[2]]`), ordered)
	if verdict != domain.ExecutionVerdictAccepted {
		t.Fatalf("ordered verdict=%s", verdict)
	}
	broken := sqliteSuite(false, sqliteInvalidJSON, true)
	verdict, _, _ = verdictFromSuiteResult(run(`[]`), broken)
	if verdict != domain.ExecutionVerdictCheckerError {
		t.Fatalf("invalid expected rows verdict=%s", verdict)
	}
}

// TestSQLiteSubmitRequiresSuite проверяет, что samples-режим без suite отклоняется.
func TestSQLiteSubmitRequiresSuite(t *testing.T) {
	t.Parallel()

	_, service, taskID, versionID := newCodeSubmissionService()
	_, err := service.Submit(context.Background(), taskID, uuid.New(), CodeSubmissionInput{
		TaskVersionID: versionID, IdempotencyKey: uuid.New(), Language: domain.ProgrammingLanguageSQLite,
		SourceFileName: sqliteFileName, SourceCode: "SELECT 1",
	})
	if err == nil {
		t.Fatal("sqlite без опубликованного набора принят в samples-режиме")
	}
}

// TestSQLiteGradedSubmitUsesSuiteReference проверяет, что graded-отправка передаёт
// ссылку на suite и версию SQLite в существующем Kafka-контракте.
func TestSQLiteGradedSubmitUsesSuiteReference(t *testing.T) {
	t.Parallel()

	repo, service, taskID, versionID := newCodeSubmissionService()
	suite := sqliteSuite(false, `[[1,"ann"],[2,null]]`, true)
	repo.suite = suite
	created, err := service.Submit(context.Background(), taskID, uuid.New(), CodeSubmissionInput{
		TaskVersionID: versionID, IdempotencyKey: uuid.New(), RunMode: domain.RunModeGraded,
		Language: domain.ProgrammingLanguageSQLite, SourceFileName: sqliteFileName, SourceCode: "SELECT * FROM users",
	})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if created.Language != domain.ProgrammingLanguageSQLite || len(repo.outbox) != 1 {
		t.Fatalf("submission=%+v outbox=%d", created, len(repo.outbox))
	}
	var request execution.RunRequest
	if err := json.Unmarshal(repo.outbox[0].Payload, &request); err != nil {
		t.Fatalf("decode outbox request: %v", err)
	}
	if request.Language.ID != "sqlite" || request.Language.Version != sqliteVersion {
		t.Fatalf("language = %+v", request.Language)
	}
	if request.Execution == nil || request.Execution.Suite == nil || request.Execution.Suite.ID != suite.Reference.ID || len(request.Execution.Inputs) != 0 {
		t.Fatalf("execution = %#v", request.Execution)
	}
	if len(request.Code.Files) != 1 || request.Code.Files[0].Path != sqliteFileName || request.Code.Entrypoint != sqliteFileName {
		t.Fatalf("code = %#v", request.Code)
	}
}

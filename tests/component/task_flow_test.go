//go:build component

package component

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	postgresadapter "github.com/overmindv/tasks/internal/adapter/postgres"
	"github.com/overmindv/tasks/internal/domain"
	"github.com/overmindv/tasks/internal/execution"
	"github.com/overmindv/tasks/internal/transport/httpapi"
	"github.com/overmindv/tasks/internal/usecase"
)

type taskPayload struct {
	ID            string          `json:"id"`
	Status        string          `json:"status"`
	TaskVersionID string          `json:"task_version_id"`
	VersionNumber int             `json:"version_number"`
	Options       []optionPayload `json:"options"`
}

type optionPayload struct {
	ID        string `json:"id"`
	IsCorrect *bool  `json:"is_correct"`
}

type submissionPayload struct {
	ID                  string `json:"id"`
	TaskVersionID       string `json:"task_version_id"`
	Correct             bool   `json:"correct"`
	TaskUpdated         bool   `json:"task_updated"`
	LatestTaskVersionID string `json:"latest_task_version_id"`
}

type historyPayload struct {
	Items []submissionPayload `json:"items"`
}

type codeSubmissionPayload struct {
	ID            string `json:"id"`
	ExecutionID   string `json:"execution_id"`
	CorrelationID string `json:"correlation_id"`
	TaskID        string `json:"task_id"`
	TaskVersionID string `json:"task_version_id"`
	Status        string `json:"status"`
	Verdict       string `json:"verdict"`
}

// TestVersionedTaskFlow проверяет полный сценарий создания, решения, обновления и удаления.
func TestVersionedTaskFlow(t *testing.T) {
	// Подготовка: DSN, pool, очистка таблиц, HTTP-handler и доверенные ID.
	dsn := os.Getenv("COMPONENT_TEST_DSN")
	if dsn == "" {
		t.Fatal("COMPONENT_TEST_DSN не задан")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New() error = %v", err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, "TRUNCATE code_execution_result_inbox, code_submission_outbox, code_submissions, submission_answers, submissions, task_options, task_versions, tasks CASCADE"); err != nil {
		t.Fatalf("truncate component database: %v", err)
	}
	store := postgresadapter.New(pool)
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	codeService := usecase.NewCodeSubmissionService(store, usecase.CodeExecutionPolicy{
		RequestsTopic:    "code-execution.requests.v1",
		TimeLimit:        time.Second,
		MemoryLimitBytes: 64 * 1024 * 1024,
	})
	mux := http.NewServeMux()
	httpapi.Register(
		mux,
		usecase.NewTaskService(store),
		usecase.NewSubmissionService(store),
		codeService,
		usecase.NewCandidateService(store),
		logger,
		"component-ingest-token",
	)

	handler := mux
	adminID := uuid.NewString()
	userID := uuid.NewString()

	// Фаза 1: создание draft с версией 1.
	created := componentTaskRequest(t, handler, http.MethodPost, "/v1/admin/tasks", adminID, "admin", map[string]any{
		"title":      "HTTP status",
		"statement":  "Какой status означает успех?",
		"task_type":  "single_choice",
		"difficulty": "easy",
		"topic_id":   nil,
		"options": []map[string]any{
			{"text": "200", "is_correct": true},
			{"text": "500", "is_correct": false},
		},
	}, http.StatusCreated)
	if created.Status != "draft" || created.VersionNumber != 1 {
		t.Fatalf("created task = %#v", created)
	}

	// Фаза 2: публикация теста.
	componentTaskRequest(t, handler, http.MethodPatch, "/v1/admin/tasks/"+created.ID+"/status", adminID, "admin", map[string]string{"status": "published"}, http.StatusOK)

	// Фаза 3: неправильный ответ не должен быть accepted.
	correctV1, wrongV1 := componentOptionIDs(t, created.Options)
	wrong := componentSubmissionRequest(t, handler, "/v1/tasks/"+created.ID+"/submissions", userID, map[string]any{
		"task_version_id":     created.TaskVersionID,
		"idempotency_key":     uuid.NewString(),
		"selected_option_ids": []string{wrongV1},
	}, http.StatusCreated)
	if wrong.Correct {
		t.Fatal("неправильный ответ не должен быть accepted")
	}

	// Фаза 4: обновление создаёт версию 2 и оставляет тест опубликованным.
	updated := componentTaskRequest(t, handler, http.MethodPut, "/v1/admin/tasks/"+created.ID, adminID, "superuser", map[string]any{
		"title":      "HTTP status updated",
		"statement":  "Какой status означает server error?",
		"task_type":  "single_choice",
		"difficulty": "easy",
		"options": []map[string]any{
			{"text": "200", "is_correct": false},
			{"text": "500", "is_correct": true},
		},
	}, http.StatusOK)
	if updated.VersionNumber != 2 || updated.TaskVersionID == created.TaskVersionID || updated.Status != "published" {
		t.Fatalf("updated task = %#v", updated)
	}

	// Фаза 5: правильный ответ по СТАРОЙ версии — корректен с task_updated=true.
	oldResult := componentSubmissionRequest(t, handler, "/v1/tasks/"+created.ID+"/submissions", userID, map[string]any{
		"task_version_id":     created.TaskVersionID,
		"idempotency_key":     uuid.NewString(),
		"selected_option_ids": []string{correctV1},
	}, http.StatusCreated)
	if !oldResult.Correct || !oldResult.TaskUpdated || oldResult.LatestTaskVersionID != updated.TaskVersionID {
		t.Fatalf("old version result = %#v", oldResult)
	}

	// Фаза 6: правильный ответ по НОВОЙ версии — корректен без task_updated.
	correctV2, _ := componentOptionIDs(t, updated.Options)
	newResult := componentSubmissionRequest(t, handler, "/v1/tasks/"+created.ID+"/submissions", userID, map[string]any{
		"task_version_id":     updated.TaskVersionID,
		"idempotency_key":     uuid.NewString(),
		"selected_option_ids": []string{correctV2},
	}, http.StatusCreated)
	if !newResult.Correct || newResult.TaskUpdated {
		t.Fatalf("new version result = %#v", newResult)
	}

	// Фаза 7: история содержит все три решения.
	history := componentHistoryRequest(t, handler, "/v1/me/submissions?task_id="+created.ID, userID)
	if len(history.Items) != 3 {
		t.Fatalf("history length = %d", len(history.Items))
	}
	programming := componentTaskRequest(t, handler, http.MethodPost, "/v1/admin/tasks", adminID, "admin", map[string]any{
		"title":       "Echo",
		"statement":   "Выведите входную строку",
		"task_type":   "programming",
		"difficulty":  "easy",
		"options":     []any{},
		"tags":        []string{"stdin"},
		"constraints": []string{"1 <= length <= 100"},
		"examples": []map[string]any{
			{"input": "hello", "output": "hello", "explanation": "echo"},
		},
	}, http.StatusCreated)
	componentTaskRequest(t, handler, http.MethodPatch, "/v1/admin/tasks/"+programming.ID+"/status", adminID, "admin", map[string]string{"status": "published"}, http.StatusOK)
	code := componentCodeSubmissionRequest(t, handler, programming, userID)
	if code.Status != "queued" || code.Verdict != "" {
		t.Fatalf("queued code submission = %#v", code)
	}
	var requestPayload string
	if err := pool.QueryRow(ctx, "SELECT payload FROM code_submission_outbox WHERE aggregate_id = $1", code.ID).Scan(&requestPayload); err != nil {
		t.Fatalf("query outbox payload: %v", err)
	}
	var requestEvent execution.RequestEvent
	if err := json.Unmarshal([]byte(requestPayload), &requestEvent); err != nil {
		t.Fatalf("decode outbox request event: %v", err)
	}
	if requestEvent.Language != domain.ProgrammingLanguagePython || len(requestEvent.Tests) != 1 || requestEvent.Tests[0].Visibility != execution.TestVisibilityOpen {
		t.Fatalf("request event = %#v", requestEvent)
	}
	claimToken := uuid.New()
	claimed, err := store.ClaimOutboxMessages(ctx, 10, claimToken, time.Now().UTC().Add(time.Minute))
	if err != nil || len(claimed) != 1 {
		t.Fatalf("ClaimOutboxMessages() = %d, %v", len(claimed), err)
	}
	if err := store.MarkOutboxPublished(ctx, claimed[0].ID, claimToken); err != nil {
		t.Fatalf("MarkOutboxPublished() error = %v", err)
	}
	var sourceCleared bool
	var payloadCleared bool
	if err := pool.QueryRow(ctx, `
		SELECT cs.source_code IS NULL, o.payload IS NULL
		FROM code_submissions cs
		INNER JOIN code_submission_outbox o ON o.aggregate_id = cs.id
		WHERE cs.id = $1`, code.ID).Scan(&sourceCleared, &payloadCleared); err != nil {
		t.Fatalf("query cleared source payload: %v", err)
	}
	if !sourceCleared || !payloadCleared {
		t.Fatalf("sourceCleared=%v payloadCleared=%v", sourceCleared, payloadCleared)
	}
	resultEvent := execution.ResultEvent{
		EventID:       uuid.New(),
		EventType:     execution.ResultEventType,
		SchemaVersion: execution.SchemaVersion,
		OccurredAt:    time.Now().UTC(),
		CorrelationID: uuid.MustParse(code.CorrelationID),
		SubmissionID:  uuid.MustParse(code.ID),
		ExecutionID:   uuid.MustParse(code.ExecutionID),
		TaskID:        uuid.MustParse(code.TaskID),
		TaskVersionID: uuid.MustParse(code.TaskVersionID),
		Verdict:       domain.ExecutionVerdictAccepted,
		Tests: []domain.ExecutionTestResult{
			{TestID: "open-1", Verdict: domain.ExecutionVerdictAccepted, Stdout: "hello"},
		},
	}
	if err := codeService.HandleResult(ctx, usecase.ExecutionMessageMetadata{
		Topic:         "code-execution.results.v1",
		Partition:     0,
		Offset:        1,
		PayloadSHA256: strings.Repeat("a", 64),
	}, resultEvent); err != nil {
		t.Fatalf("HandleResult() error = %v", err)
	}
	completedResponse := componentRawRequest(t, handler, http.MethodGet, "/v1/code-submissions/"+code.ID, userID, "", nil, http.StatusOK)
	var completed codeSubmissionPayload
	if err := json.NewDecoder(completedResponse.Body).Decode(&completed); err != nil {
		t.Fatalf("decode completed code submission: %v", err)
	}
	if completed.Status != "completed" || completed.Verdict != "accepted" {
		t.Fatalf("completed code submission = %#v", completed)
	}
	componentRawRequest(t, handler, http.MethodGet, "/v1/code-submissions/"+code.ID, uuid.NewString(), "", nil, http.StatusForbidden)
	componentRawRequest(t, handler, http.MethodDelete, "/v1/admin/tasks/"+created.ID, adminID, "admin", nil, http.StatusNoContent)
	componentRawRequest(t, handler, http.MethodPost, "/v1/tasks/"+created.ID+"/submissions", userID, "", map[string]any{
		"task_version_id":     updated.TaskVersionID,
		"idempotency_key":     uuid.NewString(),
		"selected_option_ids": []string{correctV2},
	}, http.StatusNotFound)
	componentRawRequest(t, handler, http.MethodGet, "/v1/submissions/"+oldResult.ID, userID, "", nil, http.StatusOK)
}

// componentCodeSubmissionRequest отправляет multipart-файл и декодирует queued запуск.
func componentCodeSubmissionRequest(t *testing.T, handler http.Handler, task taskPayload, userID string) codeSubmissionPayload {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	if err := writer.WriteField("task_version_id", task.TaskVersionID); err != nil {
		t.Fatalf("write task_version_id: %v", err)
	}
	if err := writer.WriteField("idempotency_key", uuid.NewString()); err != nil {
		t.Fatalf("write idempotency_key: %v", err)
	}
	if err := writer.WriteField("language", "python"); err != nil {
		t.Fatalf("write language: %v", err)
	}
	file, err := writer.CreateFormFile("file", "main.py")
	if err != nil {
		t.Fatalf("create source form file: %v", err)
	}
	if _, err := file.Write([]byte("print(input())")); err != nil {
		t.Fatalf("write source form file: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close multipart writer: %v", err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/tasks/"+task.ID+"/code-submissions", &body)
	request.Header.Set("Content-Type", writer.FormDataContentType())
	request.Header.Set("X-User-ID", userID)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("code submission status = %d, body = %s", response.Code, response.Body.String())
	}
	var payload codeSubmissionPayload
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		t.Fatalf("decode code submission response: %v", err)
	}

	return payload
}

// componentOptionIDs возвращает правильный и неправильный option ID admin-ответа.
func componentOptionIDs(t *testing.T, options []optionPayload) (string, string) {
	t.Helper()
	var correct string
	var wrong string
	for _, option := range options {
		if option.IsCorrect == nil {
			t.Fatal("admin option не содержит is_correct")
		}
		if *option.IsCorrect {
			correct = option.ID
		} else {
			wrong = option.ID
		}
	}
	if correct == "" || wrong == "" {
		t.Fatalf("options = %#v", options)
	}

	return correct, wrong
}

// componentTaskRequest выполняет запрос и декодирует задачу.
func componentTaskRequest(t *testing.T, handler http.Handler, method, path, userID, roles string, body any, status int) taskPayload {
	t.Helper()
	response := componentRawRequest(t, handler, method, path, userID, roles, body, status)
	var payload taskPayload
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		t.Fatalf("decode task response: %v", err)
	}

	return payload
}

// componentSubmissionRequest выполняет submit и декодирует результат.
func componentSubmissionRequest(t *testing.T, handler http.Handler, path, userID string, body any, status int) submissionPayload {
	t.Helper()
	response := componentRawRequest(t, handler, http.MethodPost, path, userID, "", body, status)
	var payload submissionPayload
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		t.Fatalf("decode submission response: %v", err)
	}

	return payload
}

// componentHistoryRequest получает и декодирует историю пользователя.
func componentHistoryRequest(t *testing.T, handler http.Handler, path, userID string) historyPayload {
	t.Helper()
	response := componentRawRequest(t, handler, http.MethodGet, path, userID, "", nil, http.StatusOK)
	var payload historyPayload
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		t.Fatalf("decode history response: %v", err)
	}

	return payload
}

// componentRawRequest выполняет запрос к полному HTTP handler и проверяет status.
func componentRawRequest(t *testing.T, handler http.Handler, method, path, userID, roles string, body any, status int) *httptest.ResponseRecorder {
	t.Helper()
	var requestBody io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request: %v", err)
		}
		requestBody = bytes.NewReader(data)
	}
	request := httptest.NewRequest(method, path, requestBody)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if userID != "" {
		request.Header.Set("X-User-ID", userID)
	}
	if roles != "" {
		request.Header.Set("X-User-Roles", roles)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != status {
		t.Fatalf("%s %s status = %d, want %d, body = %s", method, path, response.Code, status, response.Body.String())
	}

	return response
}

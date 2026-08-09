//go:build component

package component

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	postgresadapter "github.com/overmindv/tasks-it/internal/adapter/postgres"
	"github.com/overmindv/tasks-it/internal/transport/httpapi"
	"github.com/overmindv/tasks-it/internal/usecase"
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

// TestVersionedTaskFlow проверяет полный сценарий создания, решения, обновления и удаления.
func TestVersionedTaskFlow(t *testing.T) {
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
	if _, err := pool.Exec(ctx, "TRUNCATE submission_answers, submissions, task_options, task_versions, tasks CASCADE"); err != nil {
		t.Fatalf("truncate component database: %v", err)
	}
	store := postgresadapter.New(pool)
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	handler := httpapi.New(usecase.NewTaskService(store), usecase.NewSubmissionService(store), store, logger)
	adminID := uuid.NewString()
	userID := uuid.NewString()
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
	componentTaskRequest(t, handler, http.MethodPatch, "/v1/admin/tasks/"+created.ID+"/status", adminID, "admin", map[string]string{"status": "published"}, http.StatusOK)
	correctV1, wrongV1 := componentOptionIDs(t, created.Options)
	wrong := componentSubmissionRequest(t, handler, "/v1/tasks/"+created.ID+"/submissions", userID, map[string]any{
		"task_version_id":     created.TaskVersionID,
		"idempotency_key":     uuid.NewString(),
		"selected_option_ids": []string{wrongV1},
	}, http.StatusCreated)
	if wrong.Correct {
		t.Fatal("неправильный ответ не должен быть accepted")
	}
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
	oldResult := componentSubmissionRequest(t, handler, "/v1/tasks/"+created.ID+"/submissions", userID, map[string]any{
		"task_version_id":     created.TaskVersionID,
		"idempotency_key":     uuid.NewString(),
		"selected_option_ids": []string{correctV1},
	}, http.StatusCreated)
	if !oldResult.Correct || !oldResult.TaskUpdated || oldResult.LatestTaskVersionID != updated.TaskVersionID {
		t.Fatalf("old version result = %#v", oldResult)
	}
	correctV2, _ := componentOptionIDs(t, updated.Options)
	newResult := componentSubmissionRequest(t, handler, "/v1/tasks/"+created.ID+"/submissions", userID, map[string]any{
		"task_version_id":     updated.TaskVersionID,
		"idempotency_key":     uuid.NewString(),
		"selected_option_ids": []string{correctV2},
	}, http.StatusCreated)
	if !newResult.Correct || newResult.TaskUpdated {
		t.Fatalf("new version result = %#v", newResult)
	}
	history := componentHistoryRequest(t, handler, "/v1/me/submissions?task_id="+created.ID, userID)
	if len(history.Items) != 3 {
		t.Fatalf("history length = %d", len(history.Items))
	}
	componentRawRequest(t, handler, http.MethodDelete, "/v1/admin/tasks/"+created.ID, adminID, "admin", nil, http.StatusNoContent)
	componentRawRequest(t, handler, http.MethodPost, "/v1/tasks/"+created.ID+"/submissions", userID, "", map[string]any{
		"task_version_id":     updated.TaskVersionID,
		"idempotency_key":     uuid.NewString(),
		"selected_option_ids": []string{correctV2},
	}, http.StatusNotFound)
	componentRawRequest(t, handler, http.MethodGet, "/v1/submissions/"+oldResult.ID, userID, "", nil, http.StatusOK)
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

package httpapi

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/overmindv/tasks-it/internal/domain"
	"github.com/overmindv/tasks-it/internal/repository"
	"github.com/overmindv/tasks-it/internal/usecase"
)

type handlerRepository struct {
	repository.Repository
	detail     domain.TaskDetail
	submission domain.Submission
}

// Ping сообщает HTTP ready handler об успешной готовности.
func (r *handlerRepository) Ping(_ context.Context) error {
	return nil
}

// GetTaskDetail возвращает подготовленную задачу для transport теста.
func (r *handlerRepository) GetTaskDetail(_ context.Context, _ uuid.UUID) (domain.TaskDetail, error) {
	return r.detail, nil
}

// ListTaskDetails возвращает подготовленный список для transport теста.
func (r *handlerRepository) ListTaskDetails(_ context.Context, _ domain.TaskFilter) ([]domain.TaskDetail, error) {
	return []domain.TaskDetail{r.detail}, nil
}

// GetSubmission возвращает подготовленный результат для проверки доступа.
func (r *handlerRepository) GetSubmission(_ context.Context, _ uuid.UUID) (domain.Submission, error) {
	return r.submission, nil
}

// TestTaskResponsesHideCorrectAnswers проверяет разделение public и admin DTO.
func TestTaskResponsesHideCorrectAnswers(t *testing.T) {
	t.Parallel()
	repo, taskID, adminID := newHandlerRepository()
	handler := testHandler(repo)
	publicRequest := httptest.NewRequest(http.MethodGet, "/v1/tasks/"+taskID.String(), nil)
	publicResponse := httptest.NewRecorder()
	handler.ServeHTTP(publicResponse, publicRequest)
	if publicResponse.Code != http.StatusOK {
		t.Fatalf("public status = %d, body = %s", publicResponse.Code, publicResponse.Body.String())
	}
	if publicResponse.Header().Get(requestIDHeader) == "" {
		t.Fatal("public response должен содержать X-Request-ID")
	}
	if strings.Contains(publicResponse.Body.String(), "is_correct") {
		t.Fatalf("public response раскрывает правильный ответ: %s", publicResponse.Body.String())
	}
	adminRequest := httptest.NewRequest(http.MethodGet, "/v1/admin/tasks/"+taskID.String(), nil)
	adminRequest.Header.Set("X-User-ID", adminID.String())
	adminRequest.Header.Set("X-User-Roles", "admin")
	adminResponse := httptest.NewRecorder()
	handler.ServeHTTP(adminResponse, adminRequest)
	if adminResponse.Code != http.StatusOK || !strings.Contains(adminResponse.Body.String(), "is_correct") {
		t.Fatalf("admin response = %d, %s", adminResponse.Code, adminResponse.Body.String())
	}
}

// TestAdminEndpointRejectsRegularUser проверяет role boundary административного API.
func TestAdminEndpointRejectsRegularUser(t *testing.T) {
	t.Parallel()
	repo, _, _ := newHandlerRepository()
	handler := testHandler(repo)
	request := httptest.NewRequest(http.MethodGet, "/v1/admin/tasks", nil)
	request.Header.Set("X-User-ID", uuid.NewString())
	request.Header.Set("X-User-Roles", "student")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden || !strings.Contains(response.Body.String(), "PERMISSION_DENIED") {
		t.Fatalf("response = %d, %s", response.Code, response.Body.String())
	}
}

// TestSubmissionIsAvailableOnlyToOwner проверяет ownership сохранённого результата.
func TestSubmissionIsAvailableOnlyToOwner(t *testing.T) {
	t.Parallel()
	repo, _, _ := newHandlerRepository()
	handler := testHandler(repo)
	request := httptest.NewRequest(http.MethodGet, "/v1/submissions/"+repo.submission.ID.String(), nil)
	request.Header.Set("X-User-ID", uuid.NewString())
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden || !strings.Contains(response.Body.String(), "PERMISSION_DENIED") {
		t.Fatalf("response = %d, %s", response.Code, response.Body.String())
	}
}

// newHandlerRepository создаёт связанный набор DTO для HTTP тестов.
func newHandlerRepository() (*handlerRepository, uuid.UUID, uuid.UUID) {
	taskID := uuid.New()
	versionID := uuid.New()
	adminID := uuid.New()
	optionID := uuid.New()
	userID := uuid.New()
	currentVersionID := versionID
	repo := &handlerRepository{
		detail: domain.TaskDetail{
			Task: domain.Task{
				ID:               taskID,
				CurrentVersionID: &currentVersionID,
				Status:           domain.TaskStatusPublished,
				CreatedBy:        adminID,
				UpdatedBy:        adminID,
			},
			Version: domain.TaskVersion{
				ID:            versionID,
				TaskID:        taskID,
				VersionNumber: 1,
				Title:         "HTTP status",
				Statement:     "Какой status означает успех?",
				TaskType:      domain.TaskTypeSingleChoice,
				Difficulty:    domain.DifficultyEasy,
				Options: []domain.TaskOption{
					{ID: optionID, TaskVersionID: versionID, Text: "200", IsCorrect: true},
				},
			},
		},
		submission: domain.Submission{
			ID:                  uuid.New(),
			UserID:              userID,
			TaskID:              taskID,
			TaskVersionID:       versionID,
			TaskVersionNumber:   1,
			LatestTaskVersionID: versionID,
			LatestVersionNumber: 1,
		},
	}

	return repo, taskID, adminID
}

// testHandler собирает HTTP handler с тихим логгером.
func testHandler(repo *handlerRepository) http.Handler {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	return New(usecase.NewTaskService(repo), usecase.NewSubmissionService(repo), repo, logger)
}

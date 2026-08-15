package httpapi

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/overmindv/tasks/internal/usecase"
)

type healthChecker interface {
	Ping(ctx context.Context) error
}

// Handler объединяет зависимости внутреннего HTTP API.
type Handler struct {
	tasks       *usecase.TaskService
	submissions *usecase.SubmissionService
	code        *usecase.CodeSubmissionService
	candidates  *usecase.CandidateService
	health      healthChecker
	logger      *slog.Logger
	ingestToken string
}

// New создаёт HTTP handler со всеми маршрутами и middleware.
func New(tasksService *usecase.TaskService, submissionService *usecase.SubmissionService, codeSubmissionService *usecase.CodeSubmissionService, candidateService *usecase.CandidateService, health healthChecker, logger *slog.Logger, ingestToken string) http.Handler {
	handler := &Handler{
		tasks:       tasksService,
		submissions: submissionService,
		code:        codeSubmissionService,
		candidates:  candidateService,
		health:      health,
		logger:      logger,
		ingestToken: ingestToken,
	}
	mux := http.NewServeMux()

	// Liveness и готовность.
	mux.HandleFunc("GET /health", handler.healthHandler)
	mux.HandleFunc("GET /ready", handler.readyHandler)

	// Административный CRUD и lifecycle тестов (требует роль admin/superuser).
	mux.HandleFunc("POST /v1/admin/tasks", handler.createTask)
	mux.HandleFunc("GET /v1/admin/tasks", handler.listAdminTasks)
	mux.HandleFunc("GET /v1/admin/tasks/{id}", handler.getAdminTask)
	mux.HandleFunc("PUT /v1/admin/tasks/{id}", handler.updateTask)
	mux.HandleFunc("PATCH /v1/admin/tasks/{id}/status", handler.changeTaskStatus)
	mux.HandleFunc("DELETE /v1/admin/tasks/{id}", handler.deleteTask)
	mux.HandleFunc("GET /v1/admin/task-candidates", handler.listCandidates)
	mux.HandleFunc("GET /v1/admin/task-candidates/{id}", handler.getCandidate)
	mux.HandleFunc("PUT /v1/admin/task-candidates/{id}", handler.updateCandidate)
	mux.HandleFunc("POST /v1/admin/task-candidates/{id}/approve", handler.approveCandidate)
	mux.HandleFunc("POST /v1/admin/task-candidates/{id}/reject", handler.rejectCandidate)
	mux.HandleFunc("POST /v1/internal/task-candidates/batch", handler.importCandidates)
	mux.HandleFunc("GET /v1/tasks", handler.listPublishedTasks)
	mux.HandleFunc("GET /v1/tasks/{id}", handler.getPublishedTask)

	// Пользовательские решения.
	mux.HandleFunc("POST /v1/tasks/{id}/submissions", handler.submitAnswer)
	mux.HandleFunc("POST /v1/tasks/{id}/code-submissions", handler.submitCode)
	mux.HandleFunc("GET /v1/submissions/{id}", handler.getSubmission)
	mux.HandleFunc("GET /v1/code-submissions/{id}", handler.getCodeSubmission)
	mux.HandleFunc("GET /v1/me/submissions", handler.listMySubmissions)
	mux.HandleFunc("GET /v1/me/code-submissions", handler.listMyCodeSubmissions)

	return requestIDMiddleware(recoverMiddleware(logger, loggingMiddleware(logger, mux)))
}

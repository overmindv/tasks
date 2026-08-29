package httpapi

import (
	"log/slog"
	"net/http"

	"github.com/overmindv/tasks/internal/usecase"
)

// Router описывает минимальный контракт HTTP-роутера (parker.HTTPServer или *http.ServeMux в тестах).
type Router interface {
	Handle(pattern string, handler http.Handler)
	HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request))
}

// Handler объединяет зависимости внутреннего HTTP API.
type Handler struct {
	tasks       *usecase.TaskService
	submissions *usecase.SubmissionService
	code        *usecase.CodeSubmissionService
	candidates  *usecase.CandidateService
	logger      *slog.Logger
	ingestToken string
}

// Register регистрирует бизнес-маршруты /v1/* на роутер parker.
// Liveness/readiness/metrics/middleware предоставляет parker.
func Register(router Router, tasksService *usecase.TaskService, submissionService *usecase.SubmissionService, codeSubmissionService *usecase.CodeSubmissionService, candidateService *usecase.CandidateService, logger *slog.Logger, ingestToken string) {
	handler := &Handler{
		tasks:       tasksService,
		submissions: submissionService,
		code:        codeSubmissionService,
		candidates:  candidateService,
		logger:      logger,
		ingestToken: ingestToken,
	}

	// Административный CRUD и lifecycle тестов (требует роль admin/superuser).
	router.HandleFunc("POST /v1/admin/tasks", handler.createTask)
	router.HandleFunc("GET /v1/admin/tasks", handler.listAdminTasks)
	router.HandleFunc("GET /v1/admin/tasks/{id}", handler.getAdminTask)
	router.HandleFunc("PUT /v1/admin/tasks/{id}", handler.updateTask)
	router.HandleFunc("PATCH /v1/admin/tasks/{id}/status", handler.changeTaskStatus)
	router.HandleFunc("DELETE /v1/admin/tasks/{id}", handler.deleteTask)
	router.HandleFunc("GET /v1/admin/task-candidates", handler.listCandidates)
	router.HandleFunc("GET /v1/admin/task-candidates/{id}", handler.getCandidate)
	router.HandleFunc("PUT /v1/admin/task-candidates/{id}", handler.updateCandidate)
	router.HandleFunc("POST /v1/admin/task-candidates/{id}/approve", handler.approveCandidate)
	router.HandleFunc("POST /v1/admin/task-candidates/{id}/reject", handler.rejectCandidate)
	router.HandleFunc("POST /v1/internal/task-candidates/batch", handler.importCandidates)
	router.HandleFunc("GET /v1/tasks", handler.listPublishedTasks)
	router.HandleFunc("GET /v1/tasks/{id}", handler.getPublishedTask)

	// Пользовательские решения.
	router.HandleFunc("POST /v1/tasks/{id}/submissions", handler.submitAnswer)
	router.HandleFunc("POST /v1/tasks/{id}/code-submissions", handler.submitCode)
	router.HandleFunc("GET /v1/submissions/{id}", handler.getSubmission)
	router.HandleFunc("GET /v1/code-submissions/{id}", handler.getCodeSubmission)
	router.HandleFunc("GET /v1/me/submissions", handler.listMySubmissions)
	router.HandleFunc("GET /v1/me/code-submissions", handler.listMyCodeSubmissions)
}

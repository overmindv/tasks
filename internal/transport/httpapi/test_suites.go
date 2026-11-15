package httpapi

import (
	"log/slog"
	"net/http"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/domain"
	"github.com/overmindv/tasks/internal/usecase"
)

// RegisterSuites регистрирует только административные операции, отдельно от public DTO.
func RegisterSuites(router Router, service *usecase.SuiteService, logger *slog.Logger) {
	h := &suiteHandler{service: service, logger: logger}
	router.HandleFunc("POST /v1/admin/tasks/{id}/suite-drafts", h.create)
	router.HandleFunc("GET /v1/admin/suite-drafts/{id}", h.get)
	router.HandleFunc("PUT /v1/admin/suite-drafts/{id}", h.update)
	router.HandleFunc("DELETE /v1/admin/suite-drafts/{id}", h.delete)
	router.HandleFunc("POST /v1/admin/suite-drafts/{id}/publish", h.publish)
}

type suiteHandler struct {
	service *usecase.SuiteService
	logger  *slog.Logger
}
type suiteDraftInput struct {
	BaseVersionID uuid.UUID              `json:"base_version_id"`
	Revision      int                    `json:"revision"`
	Definition    domain.SuiteDefinition `json:"definition"`
}
type suiteRevisionInput struct {
	Revision int `json:"revision"`
}

func (h *suiteHandler) create(w http.ResponseWriter, r *http.Request) {
	if _, ok := requireAdmin(w, r); !ok {
		return
	}
	id, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	var input suiteDraftInput
	if !decodeJSON(w, r, &input) {
		return
	}
	d, err := h.service.Create(r.Context(), id, input.BaseVersionID, input.Definition)
	if err != nil {
		writeError(w, err, h.logger)
		return
	}
	writeJSON(w, 201, d)
}
func (h *suiteHandler) get(w http.ResponseWriter, r *http.Request) {
	if _, ok := requireAdmin(w, r); !ok {
		return
	}
	id, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	d, err := h.service.Get(r.Context(), id)
	if err != nil {
		writeError(w, err, h.logger)
		return
	}
	writeJSON(w, 200, d)
}
func (h *suiteHandler) update(w http.ResponseWriter, r *http.Request) {
	if _, ok := requireAdmin(w, r); !ok {
		return
	}
	id, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	var input suiteDraftInput
	if !decodeJSON(w, r, &input) {
		return
	}
	d, err := h.service.Update(r.Context(), id, input.Revision, input.Definition)
	if err != nil {
		writeError(w, err, h.logger)
		return
	}
	writeJSON(w, 200, d)
}
func (h *suiteHandler) delete(w http.ResponseWriter, r *http.Request) {
	if _, ok := requireAdmin(w, r); !ok {
		return
	}
	id, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	var input suiteRevisionInput
	if !decodeJSON(w, r, &input) {
		return
	}
	if err := h.service.Delete(r.Context(), id, input.Revision); err != nil {
		writeError(w, err, h.logger)
		return
	}
	w.WriteHeader(204)
}
func (h *suiteHandler) publish(w http.ResponseWriter, r *http.Request) {
	a, ok := requireAdmin(w, r)
	if !ok {
		return
	}
	id, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	var input suiteRevisionInput
	if !decodeJSON(w, r, &input) {
		return
	}
	s, err := h.service.Publish(r.Context(), id, input.Revision, a.UserID)
	if err != nil {
		writeError(w, err, h.logger)
		return
	}
	writeJSON(w, 200, struct {
		TaskVersionID uuid.UUID             `json:"task_version_id"`
		Reference     domain.SuiteReference `json:"reference"`
	}{s.TaskVersionID, s.Reference})
}

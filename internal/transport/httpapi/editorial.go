package httpapi

import (
	"net/http"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/domain"
	"github.com/overmindv/tasks/internal/usecase"
)

// submitEditorial принимает эталон только на административной границе.
func (h *Handler) submitEditorial(w http.ResponseWriter, r *http.Request) {
	actor, ok := requireAdmin(w, r)
	if !ok {
		return
	}
	id, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	var input struct {
		TaskVersionID  uuid.UUID                  `json:"task_version_id"`
		IdempotencyKey uuid.UUID                  `json:"idempotency_key"`
		Language       domain.ProgrammingLanguage `json:"language"`
		SourceCode     string                     `json:"source_code"`
	}
	if !decodeJSONLimit(w, r, &input, maxCodeSubmissionBody*6) {
		return
	}
	result, err := h.code.SubmitEditorial(r.Context(), id, actor.UserID, usecase.CodeSubmissionInput{
		TaskVersionID: input.TaskVersionID, IdempotencyKey: input.IdempotencyKey, Language: input.Language, SourceCode: input.SourceCode,
	}, actor.Admin)
	if err != nil {
		writeError(w, err, h.logger)

		return
	}
	// Код эталона и содержимое тестов не возвращаются в ответе постановки.
	writeJSON(w, http.StatusAccepted, struct {
		ID     uuid.UUID                   `json:"id"`
		Status domain.CodeSubmissionStatus `json:"status"`
	}{result.ID, result.Status})
}

// getEditorial возвращает только безопасный статус редакционного запуска.
func (h *Handler) getEditorial(w http.ResponseWriter, r *http.Request) {
	actor, ok := requireAdmin(w, r)
	if !ok {
		return
	}
	id, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	result, err := h.code.GetEditorial(r.Context(), id, actor.UserID, actor.Admin)
	if err != nil {
		writeError(w, err, h.logger)

		return
	}
	writeJSON(w, http.StatusOK, struct {
		ID            uuid.UUID                   `json:"id"`
		TaskVersionID uuid.UUID                   `json:"task_version_id"`
		Status        domain.CodeSubmissionStatus `json:"status"`
		Verdict       *domain.ExecutionVerdict    `json:"verdict"`
	}{result.ID, result.TaskVersionID, result.Status, result.Verdict})
}

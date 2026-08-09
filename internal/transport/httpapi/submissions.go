package httpapi

import (
	"net/http"

	"github.com/google/uuid"
	"github.com/overmindv/tasks-it/internal/apperror"
	"github.com/overmindv/tasks-it/internal/domain"
	"github.com/samber/lo"
)

// submitAnswer проверяет и сохраняет ответ текущего пользователя.
func (h *Handler) submitAnswer(w http.ResponseWriter, r *http.Request) {
	actor, ok := requireUser(w, r)
	if !ok {
		return
	}
	taskID, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	var input submissionInput
	if !decodeJSON(w, r, &input) {
		return
	}
	domainInput, err := input.domainSubmissionInput()
	if err != nil {
		writeError(w, apperror.New(apperror.ValidationError, "version, idempotency key и options должны быть UUID", http.StatusBadRequest), h.logger)

		return
	}
	result, err := h.submissions.Submit(r.Context(), taskID, actor.UserID, domainInput)
	if err != nil {
		writeError(w, err, h.logger)

		return
	}
	writeJSON(w, http.StatusCreated, responseSubmission(result))
}

// getSubmission возвращает конкретный результат владельцу или администратору.
func (h *Handler) getSubmission(w http.ResponseWriter, r *http.Request) {
	actor, ok := requireUser(w, r)
	if !ok {
		return
	}
	submissionID, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	result, err := h.submissions.Get(r.Context(), submissionID, actor.UserID, actor.Admin)
	if err != nil {
		writeError(w, err, h.logger)

		return
	}
	writeJSON(w, http.StatusOK, responseSubmission(result))
}

// listMySubmissions возвращает историю решений текущего пользователя.
func (h *Handler) listMySubmissions(w http.ResponseWriter, r *http.Request) {
	actor, ok := requireUser(w, r)
	if !ok {
		return
	}
	limit, offset, err := pagination(r)
	if err != nil {
		writeError(w, err, h.logger)

		return
	}
	var taskID *uuid.UUID
	if value := r.URL.Query().Get("task_id"); value != "" {
		id, parseErr := uuid.Parse(value)
		if parseErr != nil {
			writeError(w, apperror.New(apperror.ValidationError, "task_id должен быть UUID", http.StatusBadRequest), h.logger)

			return
		}
		taskID = &id
	}
	limit, offset = domain.NormalizePagination(limit, offset)
	items, err := h.submissions.ListMine(r.Context(), domain.SubmissionFilter{
		UserID: actor.UserID,
		TaskID: taskID,
		Limit:  limit,
		Offset: offset,
	})
	if err != nil {
		writeError(w, err, h.logger)

		return
	}
	writeJSON(w, http.StatusOK, listResponse[submissionResponse]{
		Items:  lo.Map(items, func(item domain.Submission, _ int) submissionResponse { return responseSubmission(item) }),
		Limit:  limit,
		Offset: offset,
	})
}

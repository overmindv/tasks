package httpapi

import (
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/apperror"
	"github.com/overmindv/tasks/internal/usecase"
)

type progressHandler struct {
	service *usecase.ProgressService
	logger  *slog.Logger
}

func RegisterProgress(router Router, service *usecase.ProgressService, logger *slog.Logger) {
	h := &progressHandler{service: service, logger: logger}
	router.HandleFunc("GET /v1/me/practice-progress", h.mine)
	router.HandleFunc("GET /v1/users/{id}/practice-progress", h.user)
	router.HandleFunc("PUT /v1/me/practice-progress/visibility", h.visibility)
	router.HandleFunc("GET /v1/practice-leaderboard", h.leaderboard)
}

func (h *progressHandler) mine(w http.ResponseWriter, r *http.Request) {
	actor, ok := requireUser(w, r)
	if !ok {
		return
	}
	h.get(w, r, actor.UserID, actor.UserID)
}

func (h *progressHandler) user(w http.ResponseWriter, r *http.Request) {
	actor, ok := requireUser(w, r)
	if !ok {
		return
	}
	userID, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	h.get(w, r, actor.UserID, userID)
}

func (h *progressHandler) get(w http.ResponseWriter, r *http.Request, actorID, userID uuid.UUID) {
	w.Header().Set("Cache-Control", "no-store")
	result, err := h.service.Get(r.Context(), actorID, userID)
	if errors.Is(err, usecase.ErrProgressPrivate) {
		writeJSON(w, http.StatusNotFound, apperror.New(apperror.PermissionDenied, "статистика недоступна", http.StatusNotFound))
		return
	}
	if err != nil {
		writeError(w, err, h.logger)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *progressHandler) visibility(w http.ResponseWriter, r *http.Request) {
	actor, ok := requireUser(w, r)
	if !ok {
		return
	}
	var input struct {
		Public *bool `json:"public"`
	}
	if !decodeJSON(w, r, &input) {
		return
	}
	if input.Public == nil {
		writeJSON(w, http.StatusBadRequest, apperror.New(apperror.ValidationError, "public обязателен", http.StatusBadRequest))
		return
	}
	if err := h.service.SetVisibility(r.Context(), actor.UserID, *input.Public); err != nil {
		writeError(w, err, h.logger)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, map[string]bool{"public": *input.Public})
}

func (h *progressHandler) leaderboard(w http.ResponseWriter, r *http.Request) {
	if _, ok := requireUser(w, r); !ok {
		return
	}
	limit, offset, err := pagination(r)
	if err != nil {
		writeError(w, err, h.logger)
		return
	}
	if limit == 0 {
		limit = 50
	}
	if limit < 1 || limit > 100 || offset < 0 || offset > 1000 {
		writeJSON(w, http.StatusBadRequest, apperror.New(apperror.ValidationError, "некорректная пагинация", http.StatusBadRequest))
		return
	}
	var topics *[]uuid.UUID
	switch r.URL.Query().Get("scope") {
	case "", "global":
		if r.URL.Query().Has("topic_id") {
			writeJSON(w, http.StatusBadRequest, apperror.New(apperror.ValidationError, "тема без курса", http.StatusBadRequest))
			return
		}
	case "course":
		ids := make([]uuid.UUID, 0, len(r.URL.Query()["topic_id"]))
		if len(r.URL.Query()["topic_id"]) > 1000 {
			writeJSON(w, http.StatusBadRequest, apperror.New(apperror.ValidationError, "слишком много тем", http.StatusBadRequest))
			return
		}
		seen := make(map[uuid.UUID]bool)
		for _, raw := range r.URL.Query()["topic_id"] {
			id, parseErr := uuid.Parse(strings.TrimSpace(raw))
			if parseErr != nil || id == uuid.Nil {
				writeJSON(w, http.StatusBadRequest, apperror.New(apperror.ValidationError, "некорректная тема", http.StatusBadRequest))
				return
			}
			if !seen[id] {
				ids = append(ids, id)
				seen[id] = true
			}
		}
		topics = &ids
	default:
		writeJSON(w, http.StatusBadRequest, apperror.New(apperror.ValidationError, "некорректный scope", http.StatusBadRequest))
		return
	}
	result, err := h.service.Candidates(r.Context(), topics, limit, offset)
	if err != nil {
		writeError(w, err, h.logger)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, result)
}

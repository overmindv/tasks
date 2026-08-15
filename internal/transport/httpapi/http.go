package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/apperror"
)

const maxRequestBody = 1 << 20

type actor struct {
	UserID uuid.UUID
	Admin  bool
}

// healthHandler сообщает, что процесс HTTP API запущен.
func (h *Handler) healthHandler(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// readyHandler проверяет доступность PostgreSQL.
func (h *Handler) readyHandler(w http.ResponseWriter, r *http.Request) {
	if err := h.health.Ping(r.Context()); err != nil {
		h.logger.ErrorContext(r.Context(), "проверка готовности PostgreSQL завершилась ошибкой", "error", err, "request_id", requestID(r.Context()))
		writeJSON(w, http.StatusServiceUnavailable, apperror.New(apperror.InternalError, "сервис временно не готов", http.StatusServiceUnavailable))

		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

// requireUser извлекает доверенный user ID и роли из internal headers.
func requireUser(w http.ResponseWriter, r *http.Request) (actor, bool) {
	userID, err := uuid.Parse(strings.TrimSpace(r.Header.Get("X-User-ID")))
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, apperror.New(apperror.AuthenticationRequired, "требуется аутентификация", http.StatusUnauthorized))

		return actor{}, false
	}
	roles := strings.Split(r.Header.Get("X-User-Roles"), ",")
	admin := false
	for _, role := range roles {
		role = strings.TrimSpace(role)
		if role == "admin" || role == "superuser" {
			admin = true
			break
		}
	}

	return actor{UserID: userID, Admin: admin}, true
}

// requireAdmin проверяет доверенную административную роль.
func requireAdmin(w http.ResponseWriter, r *http.Request) (actor, bool) {
	value, ok := requireUser(w, r)
	if !ok {
		return actor{}, false
	}
	if !value.Admin {
		writeJSON(w, http.StatusForbidden, apperror.New(apperror.PermissionDenied, "недостаточно прав", http.StatusForbidden))

		return actor{}, false
	}

	return value, true
}

// decodeJSON строго декодирует один JSON-объект с ограничением размера.
func decodeJSON(w http.ResponseWriter, r *http.Request, target any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBody)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		writeJSON(w, http.StatusBadRequest, apperror.New(apperror.ValidationError, "некорректный JSON", http.StatusBadRequest))

		return false
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeJSON(w, http.StatusBadRequest, apperror.New(apperror.ValidationError, "ожидался один JSON-объект", http.StatusBadRequest))

		return false
	}

	return true
}

// pagination разбирает необязательные limit и offset.
func pagination(r *http.Request) (int, int, error) {
	limit, err := queryInt(r, "limit")
	if err != nil {
		return 0, 0, err
	}
	offset, err := queryInt(r, "offset")
	if err != nil {
		return 0, 0, err
	}

	return limit, offset, nil
}

// queryInt разбирает целочисленный query-параметр.
func queryInt(r *http.Request, key string) (int, error) {
	value := r.URL.Query().Get(key)
	if value == "" {
		return 0, nil
	}
	number, err := strconv.Atoi(value)
	if err != nil {
		return 0, apperror.New(apperror.ValidationError, fmt.Sprintf("%s должен быть целым числом", key), http.StatusBadRequest)
	}

	return number, nil
}

// writeJSON записывает JSON-ответ с заданным HTTP-статусом.
func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

// writeError преобразует публичную ошибку или скрывает внутренние детали.
func writeError(w http.ResponseWriter, err error, logger *slog.Logger) {
	var public *apperror.Error
	if errors.As(err, &public) {
		writeJSON(w, public.Status, public)

		return
	}
	logger.Error("внутренняя ошибка HTTP API", "error", err)
	writeJSON(w, http.StatusInternalServerError, apperror.New(apperror.InternalError, "внутренняя ошибка сервиса", http.StatusInternalServerError))
}

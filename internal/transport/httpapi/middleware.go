package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/google/uuid"
	"github.com/overmindv/tasks-it/internal/apperror"
)

const requestIDHeader = "X-Request-ID"

type requestIDKey struct{}

type responseRecorder struct {
	http.ResponseWriter
	status int
}

// WriteHeader запоминает статус для структурного request log.
func (r *responseRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

// requestIDMiddleware принимает или создаёт стабильный request ID.
func requestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(requestIDHeader)
		if id == "" {
			id = uuid.NewString()
		}
		w.Header().Set(requestIDHeader, id)
		ctx := context.WithValue(r.Context(), requestIDKey{}, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// requestID возвращает request ID из context.
func requestID(ctx context.Context) string {
	value, _ := ctx.Value(requestIDKey{}).(string)

	return value
}

// loggingMiddleware пишет безопасный структурный лог каждого запроса.
func loggingMiddleware(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		recorder := &responseRecorder{
			ResponseWriter: w,
			status:         http.StatusOK,
		}
		next.ServeHTTP(recorder, r)
		logger.InfoContext(r.Context(), "HTTP-запрос обработан",
			"method", r.Method,
			"path", r.URL.Path,
			"status", recorder.status,
			"duration", time.Since(started),
			"request_id", requestID(r.Context()),
		)
	})
}

// recoverMiddleware скрывает panic и сохраняет stack trace только в server log.
func recoverMiddleware(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				logger.ErrorContext(r.Context(), "panic при обработке HTTP-запроса", "panic", recovered, "stack", string(debug.Stack()), "request_id", requestID(r.Context()))
				writeJSON(w, http.StatusInternalServerError, apperror.New(apperror.InternalError, "внутренняя ошибка сервиса", http.StatusInternalServerError))
			}
		}()
		next.ServeHTTP(w, r)
	})
}

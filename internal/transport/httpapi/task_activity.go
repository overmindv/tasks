package httpapi

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/apperror"
	"github.com/overmindv/tasks/internal/usecase"
)

const (
	defaultActivityPageLimit = 50
	activityNoStore          = "no-store"
	activityParamWatermark   = "watermark"
	activityParamAfter       = "after"
	activityParamLimit       = "limit"
)

type activityRoutes struct {
	service *usecase.TaskActivityService
	logger  *slog.Logger
}

// RegisterTaskActivity публикует durable-журнал публикаций для внутренних
// потребителей (Feed). Без настроенного токена маршрут не регистрируется.
func RegisterTaskActivity(router Router, service *usecase.TaskActivityService, token string, logger *slog.Logger) error {
	if token == "" {
		return nil
	}
	if len(token) < 32 || len(token) > 512 || strings.ContainsFunc(token, func(r rune) bool {
		return r < 0x21 || r > 0x7e
	}) {
		return errors.New("TASKS_ACTIVITY_TOKEN must contain 32-512 printable ASCII characters without spaces")
	}

	expected := sha256.Sum256([]byte(token))
	routes := activityRoutes{service: service, logger: logger}
	timed := http.TimeoutHandler(http.HandlerFunc(routes.changes), 5*time.Second,
		`{"code":"SERVICE_UNAVAILABLE","message":"журнал публикаций временно недоступен"}`)
	router.HandleFunc("GET /v1/internal/task-activity/changes", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", activityNoStore)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		if !activityAuthorized(r.Header.Values("Authorization"), expected) {
			writeJSON(w, http.StatusForbidden, apperror.New(apperror.PermissionDenied, "доступ запрещен", http.StatusForbidden))

			return
		}
		timed.ServeHTTP(w, r)
	})

	return nil
}

func activityAuthorized(headers []string, expected [sha256.Size]byte) bool {
	if len(headers) != 1 {
		return false
	}
	token, ok := strings.CutPrefix(headers[0], "Bearer ")
	if !ok || len(token) > 512 {
		return false
	}
	actual := sha256.Sum256([]byte(token))

	return subtle.ConstantTimeCompare(actual[:], expected[:]) == 1
}

type activityChangeResponse struct {
	Revision      int64      `json:"revision"`
	EventID       uuid.UUID  `json:"event_id"`
	EventType     string     `json:"event_type"`
	OccurredAt    time.Time  `json:"occurred_at"`
	TaskID        uuid.UUID  `json:"task_id"`
	TaskVersionID *uuid.UUID `json:"task_version_id,omitempty"`
	Title         *string    `json:"title,omitempty"`
	TopicID       *uuid.UUID `json:"topic_id,omitempty"`
	TaskType      *string    `json:"task_type,omitempty"`
	Difficulty    *string    `json:"difficulty,omitempty"`
}

type activityPageResponse struct {
	Watermark int64                    `json:"watermark"`
	Through   int64                    `json:"through"`
	More      bool                     `json:"more"`
	Changes   []activityChangeResponse `json:"changes"`
}

type activityPageQuery struct {
	watermark *int64
	after     int64
	limit     int
}

func (h activityRoutes) changes(w http.ResponseWriter, r *http.Request) {
	query, err := parseActivityQuery(r.URL.RawQuery)
	if err != nil {
		h.writeError(w, err)

		return
	}
	page, err := h.service.Changes(r.Context(), query.watermark, query.after, query.limit)
	if err != nil {
		h.writeError(w, err)

		return
	}
	changes := make([]activityChangeResponse, 0, len(page.Changes))
	for _, change := range page.Changes {
		changes = append(changes, activityChangeResponse{
			Revision: change.Revision, EventID: change.EventID, EventType: change.EventType,
			OccurredAt: change.OccurredAt.UTC(), TaskID: change.TaskID, TaskVersionID: change.TaskVersionID,
			Title: change.Title, TopicID: change.TopicID, TaskType: change.TaskType, Difficulty: change.Difficulty,
		})
	}
	writeJSON(w, http.StatusOK, activityPageResponse{
		Watermark: page.Watermark, Through: page.Through, More: page.More, Changes: changes,
	})
}

// parseActivityQuery принимает только watermark, after и limit: неизвестные
// и повторяющиеся параметры отвергаются, чтобы потребитель не читал страницу
// не в том порядке.
func parseActivityQuery(raw string) (activityPageQuery, error) {
	bad := usecase.ErrInvalidTaskActivityPage
	if len(raw) > 512 {
		return activityPageQuery{}, bad
	}
	values, err := url.ParseQuery(raw)
	if err != nil {
		return activityPageQuery{}, bad
	}
	for key, items := range values {
		if len(items) != 1 || (key != activityParamWatermark && key != activityParamAfter && key != activityParamLimit) {
			return activityPageQuery{}, bad
		}
	}
	query := activityPageQuery{limit: defaultActivityPageLimit}
	if items, ok := values[activityParamWatermark]; ok {
		watermark, err := activityRevision(items[0])
		if err != nil {
			return activityPageQuery{}, err
		}
		query.watermark = &watermark
	}
	if items, ok := values[activityParamAfter]; ok {
		after, err := activityRevision(items[0])
		if err != nil {
			return activityPageQuery{}, err
		}
		query.after = after
	}
	if items, ok := values[activityParamLimit]; ok {
		limit, err := activityRevision(items[0])
		if err != nil || limit < 1 || limit > usecase.MaxTaskActivityPage {
			return activityPageQuery{}, bad
		}
		query.limit = int(limit)
	}

	return query, nil
}

func activityRevision(raw string) (int64, error) {
	number, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || number < 0 || strconv.FormatInt(number, 10) != raw {
		return 0, usecase.ErrInvalidTaskActivityPage
	}

	return number, nil
}

func (h activityRoutes) writeError(w http.ResponseWriter, err error) {
	if errors.Is(err, usecase.ErrInvalidTaskActivityPage) {
		writeJSON(w, http.StatusBadRequest, apperror.New(apperror.ValidationError, "некорректный запрос журнала публикаций", http.StatusBadRequest))

		return
	}
	h.logger.Error("не удалось прочитать журнал публикаций задач", "error", err)
	writeJSON(w, http.StatusServiceUnavailable, apperror.New("SERVICE_UNAVAILABLE", "журнал публикаций временно недоступен", http.StatusServiceUnavailable))
}

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
	"github.com/overmindv/tasks/internal/domain"
	"github.com/overmindv/tasks/internal/usecase"
)

type resourceRoutes struct {
	service *usecase.ResourceService
	logger  *slog.Logger
}

const resourceNoStore = "no-store"

func RegisterResources(router Router, service *usecase.ResourceService, token string, logger *slog.Logger) error {
	if token == "" {
		return nil
	}
	if len(token) < 32 || len(token) > 512 || strings.ContainsFunc(token, func(r rune) bool {
		return r < 0x21 || r > 0x7e
	}) {
		return errors.New("TASKS_RESOURCE_TOKEN must contain 32-512 printable ASCII characters without spaces")
	}

	expected := sha256.Sum256([]byte(token))
	routes := resourceRoutes{service: service, logger: logger}
	register := func(pattern string, fn http.HandlerFunc) {
		timed := http.TimeoutHandler(fn, 5*time.Second, `{"code":"SERVICE_UNAVAILABLE","message":"каталог временно недоступен"}`)
		router.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", resourceNoStore)
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			if !resourceAuthorized(r.Header.Values("Authorization"), expected) {
				writeJSON(w, http.StatusForbidden, apperror.New(apperror.PermissionDenied, "доступ запрещен", http.StatusForbidden))

				return
			}
			timed.ServeHTTP(w, r)
		})
	}
	register("GET /v1/internal/resources/snapshot", routes.snapshot)
	register("GET /v1/internal/resources/changes", routes.changes)
	register("POST /v1/internal/resources/availability", routes.availability)
	register("POST /v1/internal/resources/diagnostic-eligibility", routes.diagnosticEligibility)

	return nil
}

func resourceAuthorized(headers []string, expected [sha256.Size]byte) bool {
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

type topicWeightResponse struct {
	TopicID uuid.UUID `json:"topic_id"`
	Weight  int       `json:"weight"`
}

type resourceLanguageResponse struct {
	ID      domain.ProgrammingLanguage `json:"id"`
	Version string                     `json:"version"`
}

type resourceMetadataResponse struct {
	TaskVersionID    uuid.UUID                  `json:"task_version_id"`
	Title            string                     `json:"title"`
	TaskType         domain.TaskType            `json:"task_type"`
	Difficulty       domain.Difficulty          `json:"difficulty"`
	EstimatedMinutes *int                       `json:"estimated_minutes,omitempty"`
	TopicWeights     []topicWeightResponse      `json:"topic_weights"`
	Languages        []resourceLanguageResponse `json:"languages"`
}

type resourceChangeResponse struct {
	EventID          uuid.UUID                 `json:"event_id"`
	EventType        string                    `json:"event_type"`
	SchemaVersion    int                       `json:"schema_version"`
	AggregateID      uuid.UUID                 `json:"aggregate_id"`
	AggregateVersion int64                     `json:"aggregate_version"`
	OccurredAt       time.Time                 `json:"occurred_at"`
	Tombstone        bool                      `json:"tombstone"`
	Metadata         *resourceMetadataResponse `json:"metadata,omitempty"`
}

type resourceSnapshotResponse struct {
	Watermark  int64                    `json:"watermark"`
	NextCursor *uuid.UUID               `json:"next_cursor,omitempty"`
	Resources  []resourceChangeResponse `json:"resources"`
}

type resourceChangePageResponse struct {
	Watermark int64                    `json:"watermark"`
	Through   int64                    `json:"through"`
	More      bool                     `json:"more"`
	Changes   []resourceChangeResponse `json:"changes"`
}

type resourceVersionDTO struct {
	TaskID    uuid.UUID `json:"task_id"`
	VersionID uuid.UUID `json:"task_version_id"`
}

func resourceResponses(changes []domain.ResourceChange) []resourceChangeResponse {
	result := make([]resourceChangeResponse, 0, len(changes))
	for _, c := range changes {
		row := resourceChangeResponse{
			EventID: c.EventID, EventType: "task.resource.updated", SchemaVersion: 1,
			AggregateID: c.TaskID, AggregateVersion: c.Revision, OccurredAt: c.OccurredAt.UTC(),
		}
		if c.Metadata == nil {
			row.EventType = "task.resource.removed"
			row.Tombstone = true
		} else {
			m := c.Metadata
			row.Metadata = &resourceMetadataResponse{
				TaskVersionID: m.VersionID, Title: m.Title, TaskType: m.TaskType,
				Difficulty: m.Difficulty, EstimatedMinutes: m.EstimatedMinutes,
				TopicWeights: []topicWeightResponse{{TopicID: m.TopicID, Weight: 1}},
				Languages:    []resourceLanguageResponse{},
			}
			if m.Runtime != nil {
				row.Metadata.Languages = append(row.Metadata.Languages, resourceLanguageResponse{
					ID: m.Runtime.Language, Version: m.Runtime.Version,
				})
			}
		}
		result = append(result, row)
	}

	return result
}

func (h resourceRoutes) snapshot(w http.ResponseWriter, r *http.Request) {
	query, err := parseResourceQuery(r.URL.RawQuery, "cursor")
	if err != nil {
		h.writeError(w, err)

		return
	}
	var cursor uuid.UUID
	if values, ok := query.values["cursor"]; ok {
		cursor, err = uuid.Parse(values[0])
		if err != nil || cursor == uuid.Nil || cursor.String() != values[0] {
			h.writeError(w, usecase.ErrInvalidResourcePage)

			return
		}
	}
	page, err := h.service.Snapshot(r.Context(), query.watermark, cursor, query.limit)
	if err != nil {
		h.writeError(w, err)

		return
	}
	writeJSON(w, http.StatusOK, resourceSnapshotResponse{
		Watermark: page.Watermark, NextCursor: page.NextCursor, Resources: resourceResponses(page.Records),
	})
}

func (h resourceRoutes) changes(w http.ResponseWriter, r *http.Request) {
	query, err := parseResourceQuery(r.URL.RawQuery, "after")
	if err != nil {
		h.writeError(w, err)

		return
	}
	var after int64
	if values, ok := query.values["after"]; ok {
		after, err = resourceRevision(values[0])
		if err != nil {
			h.writeError(w, err)

			return
		}
	}
	page, err := h.service.Changes(r.Context(), query.watermark, after, query.limit)
	if err != nil {
		h.writeError(w, err)

		return
	}
	writeJSON(w, http.StatusOK, resourceChangePageResponse{
		Watermark: page.Watermark, Through: page.Through, More: page.More, Changes: resourceResponses(page.Changes),
	})
}

func (h resourceRoutes) availability(w http.ResponseWriter, r *http.Request) {
	h.available(w, r, uuid.Nil)
}

func (h resourceRoutes) diagnosticEligibility(w http.ResponseWriter, r *http.Request) {
	values := r.Header.Values("X-User-ID")
	if len(values) != 1 {
		h.writeError(w, usecase.ErrInvalidResourcePage)

		return
	}
	learner, err := uuid.Parse(values[0])
	if err != nil || learner == uuid.Nil || learner.String() != values[0] {
		h.writeError(w, usecase.ErrInvalidResourcePage)

		return
	}
	h.available(w, r, learner)
}

func (h resourceRoutes) available(w http.ResponseWriter, r *http.Request, learner uuid.UUID) {
	var input struct {
		Resources []resourceVersionDTO `json:"resources"`
	}
	if !decodeJSONLimit(w, r, &input, 32<<10) {
		return
	}
	keys := make([]domain.ResourceKey, len(input.Resources))
	for i, item := range input.Resources {
		keys[i] = domain.ResourceKey{TaskID: item.TaskID, VersionID: item.VersionID}
	}
	var available []domain.ResourceKey
	var err error
	if learner == uuid.Nil {
		available, err = h.service.Available(r.Context(), keys)
	} else {
		available, err = h.service.DiagnosticEligible(r.Context(), learner, keys)
	}
	if err != nil {
		h.writeError(w, err)

		return
	}
	result := make([]resourceVersionDTO, 0, len(available))
	for _, key := range available {
		result = append(result, resourceVersionDTO{TaskID: key.TaskID, VersionID: key.VersionID})
	}
	writeJSON(w, http.StatusOK, struct {
		Resources []resourceVersionDTO `json:"resources"`
	}{Resources: result})
}

type resourcePageQuery struct {
	values    url.Values
	watermark *int64
	limit     int
}

func parseResourceQuery(raw, position string) (resourcePageQuery, error) {
	bad := usecase.ErrInvalidResourcePage
	if len(raw) > 512 {
		return resourcePageQuery{}, bad
	}
	values, err := url.ParseQuery(raw)
	if err != nil {
		return resourcePageQuery{}, bad
	}
	for key, items := range values {
		if len(items) != 1 || (key != "watermark" && key != "limit" && key != position) {
			return resourcePageQuery{}, bad
		}
	}
	query := resourcePageQuery{limit: 50, values: values}
	if items, ok := values["watermark"]; ok {
		n, err := resourceRevision(items[0])
		if err != nil {
			return resourcePageQuery{}, err
		}
		query.watermark = &n
	}
	if items, ok := values["limit"]; ok {
		n, err := resourceRevision(items[0])
		if err != nil || n < 1 || n > usecase.MaxResourcePage {
			return resourcePageQuery{}, bad
		}
		query.limit = int(n)
	}

	return query, nil
}

func resourceRevision(raw string) (int64, error) {
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n < 0 || strconv.FormatInt(n, 10) != raw {
		return 0, usecase.ErrInvalidResourcePage
	}

	return n, nil
}

func (h resourceRoutes) writeError(w http.ResponseWriter, err error) {
	if errors.Is(err, usecase.ErrInvalidResourcePage) {
		writeJSON(w, http.StatusBadRequest, apperror.New(apperror.ValidationError, "некорректный запрос каталога", http.StatusBadRequest))

		return
	}
	h.logger.Error("не удалось прочитать каталог задач", "error", err)
	writeJSON(w, http.StatusServiceUnavailable, apperror.New("SERVICE_UNAVAILABLE", "каталог временно недоступен", http.StatusServiceUnavailable))
}

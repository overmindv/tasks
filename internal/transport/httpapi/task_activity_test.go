package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/domain"
	"github.com/overmindv/tasks/internal/usecase"
)

const activityEndpoint = "/v1/internal/task-activity/changes"

type activityReadStore struct {
	watermark int64
	changes   []domain.TaskActivityChange
}

func (s *activityReadStore) TaskActivityWatermark(context.Context) (int64, error) {
	return s.watermark, nil
}

func (s *activityReadStore) TaskActivityChanges(_ context.Context, _, after int64, limit int) ([]domain.TaskActivityChange, error) {
	page := make([]domain.TaskActivityChange, 0, limit)
	for _, change := range s.changes {
		if change.Revision <= after {
			continue
		}
		if len(page) == limit {
			break
		}
		page = append(page, change)
	}

	return page, nil
}

func activityRouter(t *testing.T, store *activityReadStore, token string) *http.ServeMux {
	t.Helper()
	router := http.NewServeMux()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := RegisterTaskActivity(router, usecase.NewTaskActivityService(store), token, logger); err != nil {
		t.Fatal(err)
	}

	return router
}

func TestTaskActivityRequiresServiceToken(t *testing.T) {
	t.Parallel()

	token := strings.Repeat("x", 32)
	store := &activityReadStore{}
	disabled := http.NewServeMux()
	if err := RegisterTaskActivity(disabled, usecase.NewTaskActivityService(store), "", nil); err != nil {
		t.Fatal(err)
	}
	res := httptest.NewRecorder()
	disabled.ServeHTTP(res, httptest.NewRequest(http.MethodGet, activityEndpoint, nil))
	if res.Code != http.StatusNotFound {
		t.Fatal("журнал включён без токена:", res.Code)
	}
	if err := RegisterTaskActivity(http.NewServeMux(), usecase.NewTaskActivityService(store), "bad config", nil); err == nil {
		t.Fatal("некорректный токен принят")
	}

	router := activityRouter(t, store, token)
	for _, headers := range [][]string{
		nil,
		{"Bearer wrong"},
		{"Basic " + token},
		{"Bearer " + token, "Bearer " + token},
	} {
		req := httptest.NewRequest(http.MethodGet, activityEndpoint, nil)
		for _, header := range headers {
			req.Header.Add("Authorization", header)
		}
		res = httptest.NewRecorder()
		router.ServeHTTP(res, req)
		if res.Code != http.StatusForbidden || res.Header().Get("Cache-Control") != activityNoStore {
			t.Fatal("неавторизованный запрос журнала:", res.Code, res.Body.String())
		}
	}
}

func TestTaskActivityRejectsInvalidQuery(t *testing.T) {
	t.Parallel()

	router := activityRouter(t, &activityReadStore{}, strings.Repeat("x", 32))
	for _, raw := range []string{
		"after=-1", "after=02", "after=9223372036854775808", "watermark=-1",
		"limit=0", "limit=101", "limit=1&limit=2", "offset=2", "unknown=1",
	} {
		req := httptest.NewRequest(http.MethodGet, activityEndpoint+"?"+raw, nil)
		req.Header.Set("Authorization", "Bearer "+strings.Repeat("x", 32))
		res := httptest.NewRecorder()
		router.ServeHTTP(res, req)
		if res.Code != http.StatusBadRequest {
			t.Errorf("некорректный query %q принят: %d", raw, res.Code)
		}
	}
}

func TestTaskActivityReturnsWatermarkPage(t *testing.T) {
	t.Parallel()

	title := "Two pointers"
	taskType := "single_choice"
	difficulty := "easy"
	versionID := uuid.New()
	store := &activityReadStore{watermark: 3, changes: []domain.TaskActivityChange{
		{Revision: 1, EventID: uuid.New(), TaskID: uuid.New(), EventType: domain.TaskActivityPublished},
		{Revision: 2, EventID: uuid.New(), TaskID: uuid.New(), EventType: domain.TaskActivityPublished},
		{Revision: 3, EventID: uuid.New(), TaskID: uuid.New(), TaskVersionID: &versionID,
			EventType: domain.TaskActivityPublished, Title: &title, TaskType: &taskType, Difficulty: &difficulty},
	}}
	router := activityRouter(t, store, strings.Repeat("x", 32))

	req := httptest.NewRequest(http.MethodGet, activityEndpoint+"?limit=2", nil)
	req.Header.Set("Authorization", "Bearer "+strings.Repeat("x", 32))
	res := httptest.NewRecorder()
	router.ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("статус %d: %s", res.Code, res.Body.String())
	}
	var page activityPageResponse
	if err := json.Unmarshal(res.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if page.Watermark != 3 || page.Through != 2 || !page.More || len(page.Changes) != 2 {
		t.Fatalf("неверная страница: %+v", page)
	}

	req = httptest.NewRequest(http.MethodGet, activityEndpoint+"?watermark=3&after=2&limit=2", nil)
	req.Header.Set("Authorization", "Bearer "+strings.Repeat("x", 32))
	res = httptest.NewRecorder()
	router.ServeHTTP(res, req)
	if err := json.Unmarshal(res.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if page.More || len(page.Changes) != 1 || page.Changes[0].Revision != 3 {
		t.Fatalf("неверная последняя страница: %+v", page)
	}
	change := page.Changes[0]
	if change.EventType != domain.TaskActivityPublished || change.Title == nil || *change.Title != title ||
		change.TaskVersionID == nil || *change.TaskVersionID != versionID || change.TaskType == nil || *change.TaskType != taskType {
		t.Fatalf("неверная запись журнала: %+v", change)
	}
}

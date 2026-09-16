package httpapi

import (
	"net/http/httptest"
	"testing"

	"github.com/overmindv/tasks/internal/domain"
)

// TestTaskFilterParsesSearch проверяет, что query-параметр search попадает в фильтр и обрезается.
func TestTaskFilterParsesSearch(t *testing.T) {
	t.Parallel()
	req := httptest.NewRequest("GET", "/v1/tasks?search=%20two%20pointers%20", nil)

	filter, err := taskFilter(req, false)
	if err != nil {
		t.Fatalf("taskFilter: %v", err)
	}

	if filter.Search != "two pointers" {
		t.Fatalf("Search = %q, want %q", filter.Search, "two pointers")
	}
}

// TestTaskFilterEmptySearch проверяет, что отсутствие search даёт пустой фильтр.
func TestTaskFilterEmptySearch(t *testing.T) {
	t.Parallel()
	req := httptest.NewRequest("GET", "/v1/tasks", nil)

	filter, err := taskFilter(req, false)
	if err != nil {
		t.Fatalf("taskFilter: %v", err)
	}

	if filter.Search != "" {
		t.Fatalf("Search = %q, want empty", filter.Search)
	}
	if filter.Limit == 0 {
		t.Fatalf("pagination не применён: limit пуст")
	}
}

// гарантируем, что TaskFilter несёт поле Search (сеттер не нужен, но компиляция проверит доступность).
var _ = func() { _ = domain.TaskFilter{Search: "x"} }

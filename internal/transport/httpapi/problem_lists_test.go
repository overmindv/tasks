package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/domain"
	"github.com/overmindv/tasks/internal/repository"
)

// problemListHandlerRepository отдаёт разную видимость в зависимости от роли.
type problemListHandlerRepository struct {
	repository.Repository
	listID uuid.UUID
}

// ListProblemLists возвращает список с числом членов по уровню доступа.
func (r *problemListHandlerRepository) ListProblemLists(_ context.Context, _ int, _ int, admin bool) ([]domain.ProblemListSummary, error) {
	return []domain.ProblemListSummary{r.summary(admin)}, nil
}

// GetProblemListDetail возвращает детали списка по уровню доступа.
func (r *problemListHandlerRepository) GetProblemListDetail(_ context.Context, _ uuid.UUID, admin bool) (domain.ProblemListDetail, error) {
	summary := r.summary(admin)
	detail := domain.ProblemListDetail{ProblemList: summary.ProblemList, MemberCount: summary.MemberCount}
	if admin {
		detail.PendingCandidates = 3
	}

	return detail, nil
}

func (r *problemListHandlerRepository) summary(admin bool) domain.ProblemListSummary {
	memberCount := 1
	if admin {
		memberCount = 2
	}

	return domain.ProblemListSummary{
		ProblemList: domain.ProblemList{
			ID: r.listID, Name: "Top 100", SourceID: "leetcode",
			SourceURL: "https://leetcode.com/problem-list/abc",
		},
		MemberCount: memberCount,
	}
}

// TestProblemListVisibilityDependsOnRole проверяет, что обычный пользователь
// видит только опубликованные члены и не видит метаданные модерации.
func TestProblemListVisibilityDependsOnRole(t *testing.T) {
	t.Parallel()

	listID := uuid.New()
	handler := testHandler(&problemListHandlerRepository{listID: listID})
	userRequest := func(method, path, role string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(method, path, nil)
		request.Header.Set("X-User-ID", uuid.NewString())
		request.Header.Set("X-User-Roles", role)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)

		return response
	}

	public := userRequest(http.MethodGet, "/v1/problem-lists/"+listID.String(), "student")
	if public.Code != http.StatusOK {
		t.Fatalf("public status = %d, body = %s", public.Code, public.Body.String())
	}
	var publicDetail problemListDetailResponse
	if err := json.Unmarshal(public.Body.Bytes(), &publicDetail); err != nil {
		t.Fatal(err)
	}
	if publicDetail.MemberCount != 1 || publicDetail.PendingCandidates != 0 {
		t.Fatalf("публичная выдача раскрывает черновики: %+v", publicDetail)
	}

	admin := userRequest(http.MethodGet, "/v1/admin/problem-lists/"+listID.String(), "admin")
	if admin.Code != http.StatusOK {
		t.Fatalf("admin status = %d, body = %s", admin.Code, admin.Body.String())
	}
	var adminDetail problemListDetailResponse
	if err := json.Unmarshal(admin.Body.Bytes(), &adminDetail); err != nil {
		t.Fatal(err)
	}
	if adminDetail.MemberCount != 2 || adminDetail.PendingCandidates != 3 {
		t.Fatalf("административная выдача неполная: %+v", adminDetail)
	}

	denied := userRequest(http.MethodGet, "/v1/admin/problem-lists/"+listID.String(), "student")
	if denied.Code != http.StatusForbidden {
		t.Fatalf("административный маршрут доступен обычному пользователю: %d", denied.Code)
	}
}

package usecase

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/domain"
	"github.com/overmindv/tasks/internal/repository"
)

type problemListMemoryRepository struct {
	repository.Repository
	lists  []domain.ProblemListSummary
	detail domain.ProblemListDetail
}

func (r *problemListMemoryRepository) ListProblemLists(_ context.Context, _ int, _ int) ([]domain.ProblemListSummary, error) {
	return r.lists, nil
}

func (r *problemListMemoryRepository) GetProblemListDetail(_ context.Context, _ uuid.UUID) (domain.ProblemListDetail, error) {
	return r.detail, nil
}

// TestProblemListServiceList проверяет журнал списков с числом членов.
func TestProblemListServiceList(t *testing.T) {
	repo := &problemListMemoryRepository{lists: []domain.ProblemListSummary{
		{
			ProblemList: domain.ProblemList{
				ID: uuid.New(), Name: "oy4y273c", SourceID: "leetcode",
				SourceURL: "https://leetcode.com/problem-list/oy4y273c", CreatedAt: time.Now().UTC(),
			},
			MemberCount: 2,
		},
	}}
	service := NewProblemListService(repo)

	items, err := service.List(context.Background(), 20, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].MemberCount != 2 || items[0].Name != "oy4y273c" {
		t.Fatalf("unexpected lists: %#v", items)
	}
}

// TestProblemListServiceGet проверяет детали списка с членами.
func TestProblemListServiceGet(t *testing.T) {
	taskID := uuid.New()
	repo := &problemListMemoryRepository{detail: domain.ProblemListDetail{
		ProblemList: domain.ProblemList{
			ID: uuid.New(), Name: "Top 100", SourceID: "leetcode",
			SourceURL: "https://leetcode.com/problem-list/oy4y273c",
		},
		MemberCount:       1,
		PendingCandidates: 3,
		Members: []domain.ProblemListMember{
			{TaskID: taskID, Title: "Two Sum", Difficulty: domain.DifficultyEasy, Status: domain.TaskStatusPublished},
		},
	}}
	service := NewProblemListService(repo)

	detail, err := service.Get(context.Background(), uuid.New())
	if err != nil {
		t.Fatal(err)
	}
	if detail.PendingCandidates != 3 || len(detail.Members) != 1 || detail.Members[0].TaskID != taskID {
		t.Fatalf("unexpected detail: %#v", detail)
	}
}

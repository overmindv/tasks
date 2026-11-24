package usecase

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/domain"
)

type taskActivityStoreStub struct {
	watermark int64
	changes   []domain.TaskActivityChange
}

func (s *taskActivityStoreStub) TaskActivityWatermark(context.Context) (int64, error) {
	return s.watermark, nil
}

func (s *taskActivityStoreStub) TaskActivityChanges(_ context.Context, _, after int64, limit int) ([]domain.TaskActivityChange, error) {
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

func activityChanges(revisions ...int64) []domain.TaskActivityChange {
	changes := make([]domain.TaskActivityChange, 0, len(revisions))
	for _, revision := range revisions {
		changes = append(changes, domain.TaskActivityChange{
			Revision: revision, EventID: uuid.New(), TaskID: uuid.New(), EventType: domain.TaskActivityPublished,
		})
	}

	return changes
}

func TestTaskActivityChangesPagesOnWatermark(t *testing.T) {
	t.Parallel()

	service := NewTaskActivityService(&taskActivityStoreStub{watermark: 3, changes: activityChanges(1, 2, 3)})
	page, err := service.Changes(t.Context(), nil, 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if page.Watermark != 3 || !page.More || page.Through != 2 || len(page.Changes) != 2 {
		t.Fatalf("неверная страница: %+v", page)
	}

	next, err := service.Changes(t.Context(), &page.Watermark, page.Through, 2)
	if err != nil {
		t.Fatal(err)
	}
	if next.More || next.Through != 3 || len(next.Changes) != 1 || next.Changes[0].Revision != 3 {
		t.Fatalf("неверная последняя страница: %+v", next)
	}
}

func TestTaskActivityChangesRejectsInvalidPage(t *testing.T) {
	t.Parallel()

	service := NewTaskActivityService(&taskActivityStoreStub{watermark: 3})
	watermark := int64(2)
	for _, tc := range []struct {
		name      string
		requested *int64
		after     int64
		limit     int
	}{
		{name: "after выше watermark", after: 4, limit: 10},
		{name: "watermark ниже after", requested: &watermark, after: 3, limit: 10},
		{name: "watermark выше текущего", requested: new(int64(9)), limit: 10},
		{name: "нулевой limit", limit: 0},
		{name: "limit больше максимума", limit: MaxTaskActivityPage + 1},
		{name: "отрицательный after", after: -1, limit: 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if _, err := service.Changes(t.Context(), tc.requested, tc.after, tc.limit); !errors.Is(err, ErrInvalidTaskActivityPage) {
				t.Fatalf("ожидалась ошибка страницы, получено %v", err)
			}
		})
	}
}

package usecase

import (
	"context"
	"errors"
	"fmt"

	"github.com/overmindv/tasks/internal/domain"
)

// MaxTaskActivityPage ограничивает страницу журнала публикаций.
const MaxTaskActivityPage = 100

// ErrInvalidTaskActivityPage сообщает о некорректных параметрах страницы.
var ErrInvalidTaskActivityPage = errors.New("invalid task activity page")

// TaskActivityStore читает durable-журнал публикаций задач.
type TaskActivityStore interface {
	TaskActivityWatermark(context.Context) (int64, error)
	TaskActivityChanges(ctx context.Context, watermark, after int64, limit int) ([]domain.TaskActivityChange, error)
}

// TaskActivityService отдаёт журнал публикаций задачам-потребителям (Feed)
// постранично на одном watermark, переживая очистку Kafka и outbox.
type TaskActivityService struct {
	store TaskActivityStore
}

// NewTaskActivityService создаёт use-case журнала публикаций.
func NewTaskActivityService(store TaskActivityStore) *TaskActivityService {
	return &TaskActivityService{store: store}
}

// TaskActivityPage — страница журнала с закреплённым watermark.
type TaskActivityPage struct {
	Watermark int64
	Through   int64
	More      bool
	Changes   []domain.TaskActivityChange
}

// Changes возвращает записи после курсора в границах watermark.
// Без явного watermark страница фиксирует текущее значение, поэтому
// повтор запроса возвращает тот же результат.
func (s *TaskActivityService) Changes(ctx context.Context, requested *int64, after int64, limit int) (TaskActivityPage, error) {
	watermark, err := s.watermark(ctx, requested, after, limit)
	if err != nil {
		return TaskActivityPage{}, err
	}
	changes, err := s.store.TaskActivityChanges(ctx, watermark, after, limit+1)
	if err != nil {
		return TaskActivityPage{}, fmt.Errorf("load task activity changes: %w", err)
	}
	page := TaskActivityPage{Watermark: watermark, Through: watermark, Changes: changes}
	if len(changes) > limit {
		page.More = true
		page.Through = changes[limit-1].Revision
		page.Changes = changes[:limit]
	}

	return page, nil
}

func (s *TaskActivityService) watermark(ctx context.Context, requested *int64, after int64, limit int) (int64, error) {
	if after < 0 || limit < 1 || limit > MaxTaskActivityPage || (requested != nil && *requested < after) {
		return 0, ErrInvalidTaskActivityPage
	}
	current, err := s.store.TaskActivityWatermark(ctx)
	if err != nil {
		return 0, fmt.Errorf("locate task activity watermark: %w", err)
	}
	if after > current || (requested != nil && *requested > current) {
		return 0, ErrInvalidTaskActivityPage
	}
	if requested != nil {
		return *requested, nil
	}

	return current, nil
}

package domain

import (
	"time"

	"github.com/google/uuid"
)

// TaskActivityTopic — топик business-событий задач для ленты активностей.
const TaskActivityTopic = "task.activity.v1"

// Типы записей durable-журнала публикаций: карточка появляется при публикации
// и снимается при уходе из published (архив, возврат в draft) или удалении.
const (
	TaskActivityPublished   = "task.published"
	TaskActivityUnpublished = "task.unpublished"
)

// TaskActivityChange — запись журнала публикаций задачи. Revision растёт
// в порядке коммитов и служит watermark и курсором постраничного чтения;
// EventID остаётся стабильным идентификатором события для повторов.
type TaskActivityChange struct {
	Revision      int64
	EventID       uuid.UUID
	OccurredAt    time.Time
	TaskID        uuid.UUID
	TaskVersionID *uuid.UUID
	EventType     string
	Title         *string
	TopicID       *uuid.UUID
	TaskType      *string
	Difficulty    *string
}

// TaskActivityOutboxMessage — исходящее business-событие tasks для ленты
// активностей (топик task.activity.v1), сохраняемое атомарно с публикацией
// задачи. Отличается от OutboxMessage (выполнение кода) — использует стандартную
// parker-таблицу outbox без FK, поэтому диспетчерится отдельным диспетчером.
type TaskActivityOutboxMessage struct {
	ID    uuid.UUID // PK outbox-таблицы
	Topic string    // task.activity.v1
	Key   string    // taskID для порядка событий в партиции
	Value []byte    // taskEnvelope JSON
	// CreatedAt и метки диспетчеризации заполняются БД/диспетчером.
}

// TaskPublishedEvent формирует payload события публикации задачи.
type TaskPublishedEvent struct {
	EventID    uuid.UUID  `json:"event_id"`
	EventType  string     `json:"event_type"`
	OccurredAt time.Time  `json:"occurred_at"`
	TaskID     string     `json:"task_id"`
	Title      string     `json:"title"`
	TopicID    *uuid.UUID `json:"topic_id,omitempty"`
	TaskType   string     `json:"task_type"`
	Difficulty string     `json:"difficulty"`
}

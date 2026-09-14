package domain

import (
	"time"

	"github.com/google/uuid"
)

// TaskActivityTopic — топик business-событий задач для ленты активностей.
const TaskActivityTopic = "task.activity.v1"

// TaskActivityOutboxMessage — исходящее business-событие tasks для ленты
// активностей (топик task.activity.v1), сохраняемое атомарно с публикацией
// задачи. Отличается от OutboxMessage (выполнение кода) — использует стандартную
// parker-таблицу outbox без FK, поэтому диспетчерится отдельным диспетчером.
type TaskActivityOutboxMessage struct {
	ID    uuid.UUID // PK outbox-таблицы
	Topic string    // task.activity.v1
	Key   string    // taskID (идемпотентный ключ для feed consumer-а)
	Value []byte    // taskEnvelope JSON
	// CreatedAt и метки диспетчеризации заполняются БД/диспетчером.
}

// TaskPublishedEvent формирует payload события публикации задачи.
type TaskPublishedEvent struct {
	EventType  string     `json:"event_type"`
	OccurredAt time.Time  `json:"occurred_at"`
	TaskID     string     `json:"task_id"`
	Title      string     `json:"title"`
	TopicID    *uuid.UUID `json:"topic_id,omitempty"`
	TaskType   string     `json:"task_type"`
	Difficulty string     `json:"difficulty"`
}

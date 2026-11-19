package domain

import (
	"time"

	"github.com/google/uuid"
)

type ResourceKey struct {
	TaskID    uuid.UUID
	VersionID uuid.UUID
}

type ResourceRuntime struct {
	Language ProgrammingLanguage
	Version  string
}

type ResourceMetadata struct {
	VersionID        uuid.UUID
	TopicID          uuid.UUID
	Title            string
	TaskType         TaskType
	Difficulty       Difficulty
	EstimatedMinutes *int
	Runtime          *ResourceRuntime
}

type ResourceChange struct {
	EventID    uuid.UUID
	TaskID     uuid.UUID
	Revision   int64
	OccurredAt time.Time
	Metadata   *ResourceMetadata
}

type ResourceAvailability struct {
	Key     ResourceKey
	Runtime *ResourceRuntime
}

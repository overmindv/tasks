package usecase

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/domain"
	"github.com/overmindv/tasks/internal/repository"
)

type taskActivityCapture struct {
	repository.Repository
	messages []domain.TaskActivityOutboxMessage
}

func (c *taskActivityCapture) InsertTaskActivityOutbox(_ context.Context, event domain.TaskActivityOutboxMessage) error {
	c.messages = append(c.messages, event)

	return nil
}

func TestTaskPublicationKeepsOutboxIdentity(t *testing.T) {
	t.Parallel()

	capture := &taskActivityCapture{}
	taskID := uuid.New()
	version := domain.TaskVersion{Title: "Sorting"}
	for range 2 {
		if err := emitTaskActivityVersion(t.Context(), capture, taskID, version); err != nil {
			t.Fatal(err)
		}
	}
	first, second := capture.messages[0], capture.messages[1]
	if first.ID == second.ID {
		t.Error("two publications shared an event ID")
	}
	for _, row := range capture.messages {
		var event domain.TaskPublishedEvent
		if err := json.Unmarshal(row.Value, &event); err != nil {
			t.Fatal(err)
		}
		if event.EventID == uuid.Nil || event.EventID != row.ID || row.Key != taskID.String() ||
			event.TaskID != taskID.String() || row.Topic != domain.TaskActivityTopic {
			t.Error("task publication did not keep outbox identity", event.EventID, row.ID)
		}
	}
}

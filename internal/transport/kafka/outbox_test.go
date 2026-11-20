package kafka

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/domain"
	"github.com/overmindv/tasks/internal/repository"
)

type outboxRepository struct {
	repository.Repository
	message     domain.OutboxMessage
	marked      bool
	released    bool
	releaseCode string
}

// ClaimOutboxMessages возвращает одну подготовленную запись.
func (r *outboxRepository) ClaimOutboxMessages(_ context.Context, _ int, claimToken uuid.UUID, _ time.Time) ([]domain.OutboxMessage, error) {
	r.message.ClaimToken = &claimToken

	return []domain.OutboxMessage{r.message}, nil
}

// MarkOutboxPublished отмечает успешную публикацию.
func (r *outboxRepository) MarkOutboxPublished(_ context.Context, _, _ uuid.UUID) error {
	r.marked = true

	return nil
}

// ReleaseOutboxMessage отмечает отложенную повторную публикацию.
func (r *outboxRepository) ReleaseOutboxMessage(_ context.Context, _, _ uuid.UUID, _ time.Time, code string) error {
	r.released = true
	r.releaseCode = code

	return nil
}

type publisherStub struct {
	err     error
	topic   string
	key     string
	payload []byte
}

// Publish запоминает record или возвращает настроенную ошибку.
func (p *publisherStub) Publish(_ context.Context, topic, key string, payload []byte) error {
	p.topic = topic
	p.key = key
	p.payload = payload

	return p.err
}

// TestOutboxDispatcherConfirmsBrokerAck проверяет удаление outbox только после успешной публикации.
func TestOutboxDispatcherConfirmsBrokerAck(t *testing.T) {
	t.Parallel()
	repo := &outboxRepository{message: domain.OutboxMessage{
		ID:          uuid.New(),
		AggregateID: uuid.New(),
		Topic:       "requests",
		MessageKey:  uuid.NewString(),
		Payload:     []byte(`{"event_type":"code_execution.requested"}`),
	}}
	publisher := &publisherStub{}
	dispatcher := NewOutboxDispatcher(repo, publisher, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := dispatcher.dispatch(context.Background()); err != nil {
		t.Fatalf("dispatch() error = %v", err)
	}
	if !repo.marked || repo.released || publisher.topic != repo.message.Topic || publisher.key != repo.message.MessageKey {
		t.Fatalf("marked=%v released=%v publisher=%#v", repo.marked, repo.released, publisher)
	}
}

// TestOutboxDispatcherReleasesFailedPublish проверяет retry вместо ложного подтверждения.
func TestOutboxDispatcherReleasesFailedPublish(t *testing.T) {
	t.Parallel()
	repo := &outboxRepository{message: domain.OutboxMessage{
		ID:            uuid.New(),
		AggregateID:   uuid.New(),
		Topic:         "requests",
		MessageKey:    uuid.NewString(),
		Payload:       []byte(`{}`),
		CorrelationID: uuid.New(),
	}}
	publisher := &publisherStub{err: errors.New("broker unavailable: PRIVATE_PAYLOAD")}
	var logs bytes.Buffer
	dispatcher := NewOutboxDispatcher(repo, publisher, time.Second, slog.New(slog.NewJSONHandler(&logs, nil)))
	if err := dispatcher.dispatch(context.Background()); err != nil {
		t.Fatalf("dispatch() error = %v", err)
	}
	if repo.marked || !repo.released {
		t.Fatalf("marked=%v released=%v", repo.marked, repo.released)
	}
	if repo.releaseCode != "kafka_publish_failed" || strings.Contains(logs.String(), "PRIVATE_PAYLOAD") ||
		!strings.Contains(logs.String(), repo.message.CorrelationID.String()) {
		t.Error("incorrect redaction or missing correlation in outbox failure")
	}
}

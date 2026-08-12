package kafka

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/overmindv/tasks-it/internal/domain"
	"github.com/overmindv/tasks-it/internal/repository"
)

type outboxRepository struct {
	repository.Repository
	message  domain.OutboxMessage
	marked   bool
	released bool
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
func (r *outboxRepository) ReleaseOutboxMessage(_ context.Context, _, _ uuid.UUID, _ time.Time, _ string) error {
	r.released = true

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
		ID:          uuid.New(),
		AggregateID: uuid.New(),
		Topic:       "requests",
		MessageKey:  uuid.NewString(),
		Payload:     []byte(`{}`),
	}}
	publisher := &publisherStub{err: errors.New("broker unavailable")}
	dispatcher := NewOutboxDispatcher(repo, publisher, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := dispatcher.dispatch(context.Background()); err != nil {
		t.Fatalf("dispatch() error = %v", err)
	}
	if repo.marked || !repo.released {
		t.Fatalf("marked=%v released=%v", repo.marked, repo.released)
	}
}

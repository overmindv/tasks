package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/overmindv/tasks-it/internal/domain"
	"github.com/overmindv/tasks-it/internal/repository"
)

const (
	outboxBatchSize = 20
	outboxClaimTTL  = 30 * time.Second
)

// OutboxDispatcher доставляет сохранённые request events в Kafka at-least-once.
type OutboxDispatcher struct {
	repository   repository.Repository
	publisher    Publisher
	pollInterval time.Duration
	logger       *slog.Logger
}

// NewOutboxDispatcher создаёт dispatcher с ограниченной частотой polling.
func NewOutboxDispatcher(store repository.Repository, publisher Publisher, pollInterval time.Duration, logger *slog.Logger) *OutboxDispatcher {
	return &OutboxDispatcher{
		repository:   store,
		publisher:    publisher,
		pollInterval: pollInterval,
		logger:       logger,
	}
}

// Run публикует due messages до отмены context или ошибки repository.
func (d *OutboxDispatcher) Run(ctx context.Context) error {
	ticker := time.NewTicker(d.pollInterval)
	defer ticker.Stop()
	for {
		if err := d.dispatch(ctx); err != nil {
			return fmt.Errorf("dispatch code submission outbox: %w", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// dispatch резервирует одну пачку и независимо подтверждает каждое сообщение.
func (d *OutboxDispatcher) dispatch(ctx context.Context) error {
	claimToken := uuid.New()
	messages, err := d.repository.ClaimOutboxMessages(ctx, outboxBatchSize, claimToken, time.Now().UTC().Add(outboxClaimTTL))
	if err != nil {
		return fmt.Errorf("claim outbox batch: %w", err)
	}
	for _, message := range messages {
		if err := d.publishOne(ctx, claimToken, message); err != nil {
			return err
		}
	}

	return nil
}

// publishOne очищает payload после ack или назначает bounded retry при ошибке Kafka.
func (d *OutboxDispatcher) publishOne(ctx context.Context, claimToken uuid.UUID, message domain.OutboxMessage) error {
	err := d.publisher.Publish(ctx, message.Topic, message.MessageKey, message.Payload)
	if err == nil {
		if err := d.repository.MarkOutboxPublished(ctx, message.ID, claimToken); err != nil {
			return fmt.Errorf("confirm published outbox message %s: %w", message.ID, err)
		}
		d.logger.InfoContext(ctx, "Kafka request event опубликован",
			"event_id", message.ID,
			"submission_id", message.AggregateID,
			"topic", message.Topic,
		)

		return nil
	}
	retryAt := time.Now().UTC().Add(outboxRetryDelay(message.Attempts))
	if releaseErr := d.repository.ReleaseOutboxMessage(ctx, message.ID, claimToken, retryAt, err.Error()); releaseErr != nil {
		return fmt.Errorf("release failed outbox message %s after %v: %w", message.ID, err, releaseErr)
	}
	d.logger.WarnContext(ctx, "публикация Kafka request event отложена",
		"event_id", message.ID,
		"submission_id", message.AggregateID,
		"topic", message.Topic,
		"attempt", message.Attempts+1,
		"retry_at", retryAt,
		"error", err,
	)

	return nil
}

// outboxRetryDelay вычисляет exponential backoff с верхней границей в минуту.
func outboxRetryDelay(attempts int) time.Duration {
	if attempts < 0 {
		attempts = 0
	}
	if attempts > 5 {
		attempts = 5
	}

	return time.Second * time.Duration(1<<attempts)
}

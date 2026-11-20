package postgresadapter

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/domain"
)

// InsertOutboxMessage сохраняет Kafka-сообщение в той же транзакции, что и решение.
func (r *Postgres) InsertOutboxMessage(ctx context.Context, message domain.OutboxMessage) error {
	_, err := r.query.Exec(ctx, `
        INSERT INTO code_submission_outbox (
            id, aggregate_id, topic, message_key, payload, available_at
        ) VALUES ($1, $2, $3, $4, $5::jsonb, $6)`,
		message.ID,
		message.AggregateID,
		message.Topic,
		message.MessageKey,
		message.Payload,
		message.AvailableAt,
	)
	if err != nil {
		return fmt.Errorf("execute insert outbox message: %w", err)
	}

	return nil
}

type outboxQueue struct {
	query    querier
	learning bool
}

func NewLearningOutcomeOutbox(pool querier) *outboxQueue {
	return &outboxQueue{query: pool, learning: true}
}

func (q outboxQueue) table() string {
	if q.learning {
		return "learning_outcome_outbox"
	}

	return "code_submission_outbox"
}

func (r *Postgres) ClaimOutboxMessages(ctx context.Context, limit int, claimToken uuid.UUID, claimedUntil time.Time) ([]domain.OutboxMessage, error) {
	return (outboxQueue{query: r.query}).ClaimOutboxMessages(ctx, limit, claimToken, claimedUntil)
}

// ClaimOutboxMessages резервирует пачку, не удерживая транзакцию во время публикации.
func (q outboxQueue) ClaimOutboxMessages(ctx context.Context, limit int, claimToken uuid.UUID, claimedUntil time.Time) ([]domain.OutboxMessage, error) {
	table := q.table()
	rows, err := q.query.Query(ctx, `
        WITH due AS (
            SELECT id
            FROM `+table+`
            WHERE published_at IS NULL
              AND available_at <= now()
              AND (claimed_until IS NULL OR claimed_until < now())
            ORDER BY available_at, created_at, id
            FOR UPDATE SKIP LOCKED
            LIMIT $1
        )
        UPDATE `+table+` o
        SET claim_token = $2,
            claimed_until = $3,
            updated_at = now()
        FROM due
        WHERE o.id = due.id
        RETURNING o.id, o.aggregate_id, o.topic, o.message_key, o.payload::text,
                  o.attempts, o.claim_token, o.claimed_until, o.available_at, o.created_at,
                  (SELECT correlation_id FROM code_submissions WHERE id = o.aggregate_id)`,
		limit,
		claimToken,
		claimedUntil,
	)
	if err != nil {
		return nil, fmt.Errorf("claim outbox messages: %w", err)
	}
	defer rows.Close()
	messages := make([]domain.OutboxMessage, 0, limit)
	for rows.Next() {
		var message domain.OutboxMessage
		var payload string
		if err := rows.Scan(
			&message.ID,
			&message.AggregateID,
			&message.Topic,
			&message.MessageKey,
			&payload,
			&message.Attempts,
			&message.ClaimToken,
			&message.ClaimedUntil,
			&message.AvailableAt,
			&message.CreatedAt,
			&message.CorrelationID,
		); err != nil {
			return nil, fmt.Errorf("scan claimed outbox message: %w", err)
		}
		message.Payload = []byte(payload)
		messages = append(messages, message)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate claimed outbox messages: %w", err)
	}

	return messages, nil
}

func (r *Postgres) MarkOutboxPublished(ctx context.Context, id, claimToken uuid.UUID) error {
	return (outboxQueue{query: r.query}).MarkOutboxPublished(ctx, id, claimToken)
}

func (q outboxQueue) MarkOutboxPublished(ctx context.Context, id, claimToken uuid.UUID) error {
	tag, err := q.query.Exec(ctx, `
		UPDATE `+q.table()+`
		SET payload = NULL,
			published_at = now(),
			claim_token = NULL,
			claimed_until = NULL,
			last_error = NULL,
			updated_at = now()
		WHERE id = $1 AND claim_token = $2 AND published_at IS NULL`, id, claimToken)
	if err != nil {
		return fmt.Errorf("mark outbox message published: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("mark outbox message published: claim lost for %s", id)
	}

	return nil
}

func (r *Postgres) ReleaseOutboxMessage(ctx context.Context, id, claimToken uuid.UUID, availableAt time.Time, lastError string) error {
	return (outboxQueue{query: r.query}).ReleaseOutboxMessage(ctx, id, claimToken, availableAt, lastError)
}

func (q outboxQueue) ReleaseOutboxMessage(ctx context.Context, id, claimToken uuid.UUID, availableAt time.Time, lastError string) error {
	if len(lastError) > 1000 {
		lastError = lastError[:1000]
	}
	tag, err := q.query.Exec(ctx, `
        UPDATE `+q.table()+`
        SET attempts = attempts + 1,
            available_at = $3,
            claim_token = NULL,
            claimed_until = NULL,
            last_error = $4,
            updated_at = now()
        WHERE id = $1 AND claim_token = $2 AND published_at IS NULL`, id, claimToken, availableAt, lastError)
	if err != nil {
		return fmt.Errorf("release outbox message: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("release outbox message: claim lost for %s", id)
	}

	return nil
}

// InsertExecutionInbox регистрирует Kafka offset и event ID для идемпотентной обработки результата.
func (r *Postgres) InsertExecutionInbox(ctx context.Context, record domain.ExecutionInboxRecord) (bool, error) {
	tag, err := r.query.Exec(ctx, `
        INSERT INTO code_execution_result_inbox (
            id, event_id, topic, partition, message_offset, payload_sha256, status, error_code
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT DO NOTHING`,
		record.ID,
		record.EventID,
		record.Topic,
		record.Partition,
		record.Offset,
		record.PayloadSHA256,
		record.Status,
		nullableString(record.ErrorCode),
	)
	if err != nil {
		return false, fmt.Errorf("execute insert execution inbox: %w", err)
	}

	return tag.RowsAffected() == 1, nil
}

// nullableString преобразует пустую строку в SQL NULL.
func nullableString(value string) any {
	if value == "" {
		return nil
	}

	return value
}

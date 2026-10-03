package kafka

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/execution"
	"github.com/overmindv/tasks/internal/usecase"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ResultConsumer читает финальные ответы sandbox и подтверждает offset после PostgreSQL commit.
type ResultConsumer struct {
	client  *kgo.Client
	service *usecase.CodeSubmissionService
	logger  *slog.Logger
}

// resultHandler задаёт контракт приёма одного Kafka result event с точки зрения
// consumer'а (реализуется *usecase.CodeSubmissionService). Инъекция интерфейса вместо
// конкретного сервиса делает логику processResult тестируемой без реального
// Kafka-кластера и PostgreSQL.
type resultHandler interface {
	HandleRunResult(ctx context.Context, metadata usecase.ExecutionMessageMetadata, result execution.RunResult) error
	RejectResult(ctx context.Context, metadata usecase.ExecutionMessageMetadata, eventID *uuid.UUID, errorCode string) error
}

// NewResultConsumer создаёт обработчик result topic поверх настроенного group client.
func NewResultConsumer(client *kgo.Client, service *usecase.CodeSubmissionService, logger *slog.Logger) *ResultConsumer {
	return &ResultConsumer{
		client:  client,
		service: service,
		logger:  logger,
	}
}

// Run последовательно обрабатывает records, чтобы commit не обгонял запись результата.
// Транзиентные ошибки Kafka (rebalance, join группы, недоступность координатора) и БД
// не должны ронять сервис: после backoff опрос продолжается. Выход — только по отмене
// context (graceful shutdown). Доставка остаётся at-least-once.
func (c *ResultConsumer) Run(ctx context.Context) error {
	const (
		initialRetry = time.Second
		maxRetry     = 30 * time.Second
	)
	retry := initialRetry
	for {
		fetches := c.client.PollRecords(ctx, 1)
		if ctx.Err() != nil {
			return nil
		}
		var err error
		if errs := fetches.Errors(); len(errs) > 0 {
			err = fmt.Errorf("poll Kafka results: %w", errs[0].Err)
			c.client.AllowRebalance()
		} else {
			records := fetches.Records()
			if len(records) == 0 {
				c.client.AllowRebalance()
				retry = initialRetry
				continue
			}
			record := records[0]
			if processErr := c.process(ctx, record); processErr != nil {
				err = fmt.Errorf("process Kafka result topic=%s partition=%d offset=%d: %w", record.Topic, record.Partition, record.Offset, processErr)
				c.client.AllowRebalance()
			} else if commitErr := c.client.CommitRecords(ctx, record); commitErr != nil {
				err = fmt.Errorf("commit Kafka result offset: %w", commitErr)
				c.client.AllowRebalance()
			} else {
				c.client.AllowRebalance()
				retry = initialRetry
				continue
			}
		}
		c.logger.WarnContext(ctx, "Kafka result consumer: временная ошибка, повтор", "error", err, "retry_after", retry)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(retry):
		}
		if retry < maxRetry {
			retry *= 2
		}
	}
}

// process валидирует событие либо сохраняет безопасную rejected inbox запись.
func (c *ResultConsumer) process(ctx context.Context, record *kgo.Record) error {
	return processResult(ctx, record, c.service, c.logger)
}

// processResult принимает решение по одному record: обработать результат, отклонить
// невалидное/несовместимое событие (rejected inbox запись, commit-путь) либо вернуть
// ошибку, чтобы Run повторил обработку (retry-путь при транзиентной ошибке сервиса).
// Вынесена отдельно, чтобы покрыть все ветки unit-тестами на фейковом resultHandler.
func processResult(ctx context.Context, record *kgo.Record, handler resultHandler, logger *slog.Logger) error {
	metadata := usecase.ExecutionMessageMetadata{
		Topic:         record.Topic,
		Partition:     record.Partition,
		Offset:        record.Offset,
		PayloadSHA256: payloadHash(record.Value),
	}
	result, err := execution.DecodeRunResult(record.Value)
	if err != nil {
		attemptID, errorCode := rejectedEnvelope(record.Value)
		if rejectErr := handler.RejectResult(ctx, metadata, attemptID, errorCode); rejectErr != nil {
			return fmt.Errorf("persist rejected execution result: %w", rejectErr)
		}
		logger.WarnContext(ctx, "невалидный Kafka result event отклонён",
			"topic", record.Topic,
			"partition", record.Partition,
			"offset", record.Offset,
			"error_code", errorCode,
		)

		return nil
	}
	if err := handler.HandleRunResult(ctx, metadata, result); err != nil {
		if usecase.IsPermanentResultError(err) {
			var attemptID *uuid.UUID
			if parsed, parseErr := uuid.Parse(result.AttemptID); parseErr == nil {
				attemptID = &parsed
			}
			if rejectErr := handler.RejectResult(ctx, metadata, attemptID, execution.InboxErrorInvalidEvent); rejectErr != nil {
				return fmt.Errorf("persist mismatched execution result after %v: %w", err, rejectErr)
			}
			logger.WarnContext(ctx, "Kafka result event не соответствует запуску",
				"submission_id", result.SubmissionID,
				"attempt_id", result.AttemptID,
				"topic", record.Topic,
				"partition", record.Partition,
				"offset", record.Offset,
			)

			return nil
		}

		return fmt.Errorf("handle execution result: %w", err)
	}
	logger.InfoContext(ctx, "Kafka result event обработан",
		"submission_id", result.SubmissionID,
		"attempt_id", result.AttemptID,
		"topic", record.Topic,
		"partition", record.Partition,
		"offset", record.Offset,
	)

	return nil
}

// payloadHash вычисляет fingerprint без сохранения или логирования пользовательского вывода.
func payloadHash(payload []byte) string {
	sum := sha256.Sum256(payload)

	return hex.EncodeToString(sum[:])
}

// rejectedEnvelope извлекает attempt ID и отличает неизвестную версию контракта.
func rejectedEnvelope(payload []byte) (*uuid.UUID, string) {
	var envelope struct {
		SchemaVersion string `json:"schema_version"`
		AttemptID     string `json:"attempt_id"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return nil, execution.InboxErrorInvalidEvent
	}
	var attemptID *uuid.UUID
	if parsed, err := uuid.Parse(envelope.AttemptID); err == nil && parsed != uuid.Nil {
		attemptID = &parsed
	}
	if envelope.SchemaVersion != execution.ResultSchemaVersion {
		return attemptID, execution.InboxErrorUnsupportedEvent
	}

	return attemptID, execution.InboxErrorInvalidEvent
}

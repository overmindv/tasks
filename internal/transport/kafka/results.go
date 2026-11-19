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
	client  resultClient
	service resultHandler
	logger  *slog.Logger
}

// resultClient ограничивает транспорт операциями последовательного consumer.
type resultClient interface {
	PollRecords(context.Context, int) kgo.Fetches
	CommitRecords(context.Context, ...*kgo.Record) error
	AllowRebalance()
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

// Run последовательно обрабатывает records и ограничивает удержание partition.
// При исчерпании бюджета caller обязан закрыть клиент перед новым запуском:
// продолжать poll тем же клиентом нельзя, иначе незафиксированный offset потеряется.
func (c *ResultConsumer) Run(ctx context.Context) error {
	return c.run(ctx, resultRetryBudget)
}

func (c *ResultConsumer) run(ctx context.Context, budget time.Duration) error {
	defer c.client.AllowRebalance()
	retry := time.Second
	for {
		fetches := c.client.PollRecords(ctx, 1)
		if ctx.Err() != nil {
			return nil
		}
		// Fetches могут одновременно содержать записи и ошибки других partition.
		// Не выбрасываем уже выданную запись из-за такой ошибки.
		records := fetches.Records()
		if len(records) > 0 {
			record := records[0]
			recordCtx, cancel := context.WithTimeout(ctx, budget)
			err := c.processAndCommit(recordCtx, record)
			cancel()
			if ctx.Err() != nil {
				return nil
			}
			if err != nil {
				return fmt.Errorf("kafka result retry budget exhausted topic=%s partition=%d offset=%d: %w", record.Topic, record.Partition, record.Offset, err)
			}
			c.client.AllowRebalance()
			retry = time.Second
			continue
		}
		c.client.AllowRebalance()
		if errs := fetches.Errors(); len(errs) > 0 {
			// Не включаем произвольные ошибки транспорта в логи с пользовательскими данными.
			c.logger.WarnContext(ctx, "Kafka result poll failed", "retry_after", retry)
			if err := waitResultRetry(ctx, retry); err != nil {
				return nil
			}
			retry = min(retry*2, 5*time.Second)
		} else {
			retry = time.Second
		}
	}
}

// processAndCommit удерживает одну запись до commit. Успешную транзакцию БД
// повторять при ошибке Kafka не нужно; после перезапуска дубли поглощает inbox.
func (c *ResultConsumer) processAndCommit(ctx context.Context, record *kgo.Record) error {
	processed := false
	retry := time.Second
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		var err error
		stage := "process"
		if !processed {
			err = c.process(ctx, record)
			processed = err == nil
		}
		if processed {
			if err := ctx.Err(); err != nil {
				return err
			}
			stage = "commit"
			err = c.client.CommitRecords(ctx, record)
		}
		if err == nil {
			return nil
		}
		c.logger.WarnContext(ctx, "Kafka result retry",
			"stage", stage, "topic", record.Topic, "partition", record.Partition,
			"offset", record.Offset, "retry_after", retry)
		if err := waitResultRetry(ctx, retry); err != nil {
			return err
		}
		retry = min(retry*2, 5*time.Second)
	}
}

func waitResultRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
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
				return fmt.Errorf("persist mismatched execution result: %w", rejectErr)
			}
			// Идентификаторы отвергнутого envelope не доверенные: логируем только координаты Kafka.
			logger.WarnContext(ctx, "Kafka result event не соответствует запуску",
				"topic", record.Topic,
				"partition", record.Partition,
				"offset", record.Offset,
			)

			return nil
		}

		return fmt.Errorf("handle execution result: %w", err)
	}
	if correlation, err := uuid.Parse(result.TraceID); err == nil && correlation != uuid.Nil {
		logger.InfoContext(ctx, "Kafka result event обработан",
			"correlation_id", correlation,
			"topic", record.Topic,
			"partition", record.Partition,
			"offset", record.Offset,
		)
	}

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

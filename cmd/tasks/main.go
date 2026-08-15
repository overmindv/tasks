package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	postgresadapter "github.com/overmindv/tasks/internal/adapter/postgres"
	"github.com/overmindv/tasks/internal/config"
	"github.com/overmindv/tasks/internal/transport/httpapi"
	kafkaadapter "github.com/overmindv/tasks/internal/transport/kafka"
	"github.com/overmindv/tasks/internal/usecase"
)

type healthChecks struct {
	postgres interface {
		Ping(ctx context.Context) error
	}
	kafka interface {
		Ping(ctx context.Context) error
	}
}

// Ping проверяет все обязательные зависимости readiness endpoint.
func (h healthChecks) Ping(ctx context.Context) error {
	if err := h.postgres.Ping(ctx); err != nil {
		return fmt.Errorf("postgresql недоступен: %w", err)
	}
	if err := h.kafka.Ping(ctx); err != nil {
		return fmt.Errorf("kafka недоступна: %w", err)
	}

	return nil
}

// main загружает зависимости и запускает внутренний HTTP API tasks.
func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("не удалось загрузить конфигурацию", "error", err)
		os.Exit(1)
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel(cfg.LogLevel)}))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.Error("не удалось создать PostgreSQL pool", "error", err)
		os.Exit(1)
	}
	defer pool.Close()
	store := postgresadapter.New(pool)
	producer, err := kafkaadapter.NewProducer(cfg.KafkaBrokers)
	if err != nil {
		logger.Error("не удалось создать Kafka producer", "error", err)
		os.Exit(1)
	}
	defer producer.Close()
	consumerClient, err := kafkaadapter.NewConsumerClient(cfg.KafkaBrokers, cfg.KafkaResultsTopic, cfg.KafkaResultsGroup)
	if err != nil {
		logger.Error("не удалось создать Kafka result consumer", "error", err)
		os.Exit(1)
	}
	defer consumerClient.Close()
	taskService := usecase.NewTaskService(store)
	submissionService := usecase.NewSubmissionService(store)
	codeSubmissionService := usecase.NewCodeSubmissionService(store, usecase.CodeExecutionPolicy{
		RequestsTopic:    cfg.KafkaRequestsTopic,
		TimeLimit:        cfg.CodeExecutionTimeout,
		MemoryLimitBytes: cfg.CodeExecutionMemory,
	})
	candidateService := usecase.NewCandidateService(store)
	health := healthChecks{
		postgres: store,
		kafka:    producer,
	}
	server := &http.Server{
		Addr:              cfg.HTTPAddress,
		Handler:           httpapi.New(taskService, submissionService, codeSubmissionService, candidateService, health, logger, cfg.TaskHunterIngestToken),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       60 * time.Second,
	}
	dispatcher := kafkaadapter.NewOutboxDispatcher(store, producer, cfg.OutboxPollInterval, logger)
	consumer := kafkaadapter.NewResultConsumer(consumerClient, codeSubmissionService, logger)
	go serve(server, logger, stop, cfg)
	go runWorker(ctx, "Kafka outbox dispatcher", dispatcher.Run, logger, stop)
	go runWorker(ctx, "Kafka result consumer", consumer.Run, logger, stop)
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("не удалось корректно остановить HTTP-сервер", "error", err)
	}
}

// runWorker останавливает процесс при неожиданной ошибке фонового worker'а.
func runWorker(ctx context.Context, name string, run func(context.Context) error, logger *slog.Logger, stop context.CancelFunc) {
	if err := run(ctx); err != nil && ctx.Err() == nil {
		logger.Error(name+" завершился с ошибкой", "error", err)
		stop()
	}
}

// serve запускает сервер и останавливает приложение при неожиданной ошибке.
func serve(server *http.Server, logger *slog.Logger, stop context.CancelFunc, cfg config.Config) {
	logger.Info("tasks запущен", "address", cfg.HTTPAddress, "environment", cfg.Environment)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("HTTP-сервер завершился с ошибкой", "error", err)
		stop()
	}
}

// logLevel преобразует строковую настройку в slog level.
func logLevel(value string) slog.Level {
	switch value {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	postgresadapter "github.com/overmindv/tasks-it/internal/adapter/postgres"
	"github.com/overmindv/tasks-it/internal/config"
	"github.com/overmindv/tasks-it/internal/transport/httpapi"
	"github.com/overmindv/tasks-it/internal/usecase"
)

// main загружает зависимости и запускает внутренний HTTP API tasks-it.
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
	taskService := usecase.NewTaskService(store)
	submissionService := usecase.NewSubmissionService(store)
	candidateService := usecase.NewCandidateService(store)
	server := &http.Server{
		Addr:              cfg.HTTPAddress,
		Handler:           httpapi.New(taskService, submissionService, candidateService, store, logger, cfg.TaskHunterIngestToken),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       60 * time.Second,
	}
	go serve(server, logger, stop, cfg)
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("не удалось корректно остановить HTTP-сервер", "error", err)
	}
}

// serve запускает сервер и останавливает приложение при неожиданной ошибке.
func serve(server *http.Server, logger *slog.Logger, stop context.CancelFunc, cfg config.Config) {
	logger.Info("tasks-it запущен", "address", cfg.HTTPAddress, "environment", cfg.Environment)
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

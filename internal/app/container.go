package app

import (
	"context"
	"log/slog"
	"time"

	"github.com/overmindv/parker"
	postgresadapter "github.com/overmindv/tasks/internal/adapter/postgres"
	"github.com/overmindv/tasks/internal/config"
	"github.com/overmindv/tasks/internal/domain"
	"github.com/overmindv/tasks/internal/transport/httpapi"
	kafkaadapter "github.com/overmindv/tasks/internal/transport/kafka"
	"github.com/overmindv/tasks/internal/usecase"
)

// Build выполняет wiring бизнес-зависимостей tasks на каркас parker:
// открывает базу, настраивает Kafka (producer + result consumer), регистрирует
// HTTP-роуты /v1/*, outbox-dispatcher и result-consumer как фоновые Runnable.
// HTTP/middleware/метрики/health/миграции берёт на себя parker.
func Build(app *parker.App) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	pool, err := app.Postgres() // добавляет health-чек "postgres" в /ready
	if err != nil {
		return err
	}

	store := postgresadapter.New(pool)

	producer, err := app.NewProducer()
	if err != nil {
		return err
	}
	app.AddHealthCheck("kafka", parker.HealthCheckFunc(producer.Ping))

	// Прямой consumer-group клиент для чтения финальных результатов sandbox:
	// специфичная логика commit/retry остаётся в tasks, поэтому не parker.Subscriber.
	consumerClient, err := kafkaadapter.NewConsumerClient(cfg.KafkaBrokers, cfg.KafkaResultsTopic, cfg.KafkaResultsGroup)
	if err != nil {
		return err
	}

	taskService := usecase.NewTaskService(store)
	submissionService := usecase.NewSubmissionService(store)
	codeSubmissionService := usecase.NewCodeSubmissionService(store, usecase.CodeExecutionPolicy{
		RequestsTopic:    cfg.KafkaRequestsTopic,
		TimeLimit:        cfg.CodeExecutionTimeout,
		MemoryLimitBytes: cfg.CodeExecutionMemory,
		MaxAttempts:      cfg.ExecutionMaxAttempts,
		RetryGrace:       cfg.ExecutionRetryGrace,
		PIDs:             cfg.ExecutionPIDs,
		StdoutBytes:      cfg.ExecutionStdoutBytes,
		StderrBytes:      cfg.ExecutionStderrBytes,
		WorkspaceBytes:   cfg.ExecutionWorkspaceBytes,
		LanguageVersions: map[domain.ProgrammingLanguage]string{
			domain.ProgrammingLanguagePython: cfg.PythonVersion,
			domain.ProgrammingLanguageGo:     cfg.GoVersion,
		},
	})
	candidateService := usecase.NewCandidateService(store)
	progressService := usecase.NewProgressService(store)

	httpapi.Register(
		app.HTTP(),
		taskService,
		submissionService,
		codeSubmissionService,
		candidateService,
		app.Logger(),
		cfg.TaskHunterIngestToken,
	)
	httpapi.RegisterProgress(app.HTTP(), progressService, app.Logger())

	dispatcher := kafkaadapter.NewOutboxDispatcher(store, producer, cfg.OutboxPollInterval, app.Logger())
	consumer := kafkaadapter.NewResultConsumer(consumerClient, codeSubmissionService, app.Logger())

	// Второй диспетчер — business-события задач (task.published) через стандартную
	// parker-таблицу outbox в топик активности ленты.
	activityDispatcher := parker.NewOutboxDispatcher(parker.NewPgOutbox(pool), producer, cfg.OutboxPollInterval, app.Logger())
	app.AddHealthCheck("kafka-activity", parker.HealthCheckFunc(producer.Ping))

	app.AddRunnable("kafka-outbox", dispatcher.Run)
	app.AddRunnable("task-activity-outbox", activityDispatcher.Run)
	app.AddRunnable("kafka-results", func(ctx context.Context) error {
		defer consumerClient.Close()
		return consumer.Run(ctx)
	})
	app.AddRunnable("code-submission-reaper", func(ctx context.Context) error {
		return runReaper(ctx, codeSubmissionService, cfg.ExecutionReaperInterval, app.Logger())
	})

	return nil
}

// runReaper периодически переотправляет заброшенные queued-запуски или завершает их
// как infrastructure_error по исчерпании попыток. Транзиентные ошибки БД не роняют
// сервис: после паузы прогон повторяется.
func runReaper(ctx context.Context, service *usecase.CodeSubmissionService, interval time.Duration, logger *slog.Logger) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := service.RequeueStaleCodeSubmissions(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			logger.ErrorContext(ctx, "reaper: временная ошибка, повтор", "error", err, "retry_after", interval)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(interval):
			}
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

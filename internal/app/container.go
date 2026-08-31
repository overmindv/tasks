package app

import (
	"context"

	"github.com/overmindv/parker"
	postgresadapter "github.com/overmindv/tasks/internal/adapter/postgres"
	"github.com/overmindv/tasks/internal/config"
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
	})
	candidateService := usecase.NewCandidateService(store)

	httpapi.Register(
		app.HTTP(),
		taskService,
		submissionService,
		codeSubmissionService,
		candidateService,
		app.Logger(),
		cfg.TaskHunterIngestToken,
	)

	dispatcher := kafkaadapter.NewOutboxDispatcher(store, producer, cfg.OutboxPollInterval, app.Logger())
	consumer := kafkaadapter.NewResultConsumer(consumerClient, codeSubmissionService, app.Logger())

	app.AddRunnable("kafka-outbox", dispatcher.Run)
	app.AddRunnable("kafka-results", func(ctx context.Context) error {
		defer consumerClient.Close()
		return consumer.Run(ctx)
	})

	return nil
}

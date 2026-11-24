package app

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/overmindv/parker"
	postgresadapter "github.com/overmindv/tasks/internal/adapter/postgres"
	"github.com/overmindv/tasks/internal/adapter/topiccatalog"
	"github.com/overmindv/tasks/internal/config"
	"github.com/overmindv/tasks/internal/domain"
	"github.com/overmindv/tasks/internal/observability"
	"github.com/overmindv/tasks/internal/transport/httpapi"
	kafkaadapter "github.com/overmindv/tasks/internal/transport/kafka"
	"github.com/overmindv/tasks/internal/usecase"

	"github.com/overmindv/tasks/internal/adapter/suitestore"
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

	var executionMetrics *observability.Execution
	if cfg.ExecutionMetricsToken != "" {
		if cfg.ExecutionMetricsToken == cfg.TaskHunterIngestToken {
			return errors.New("TASKS_EXECUTION_METRICS_TOKEN must differ from TASK_HUNTER_INGEST_TOKEN")
		}
		if cfg.ExecutionMetricsToken == cfg.ResourceToken {
			return errors.New("TASKS_EXECUTION_METRICS_TOKEN must differ from TASKS_RESOURCE_TOKEN")
		}
		executionMetrics = observability.NewExecution()
		handler, handlerErr := executionMetrics.Handler(cfg.ExecutionMetricsToken)
		if handlerErr != nil {
			return handlerErr
		}
		app.HTTP().Handle(http.MethodGet+" "+observability.ExecutionMetricsPath, handler)
	}

	pool, err := app.Postgres() // добавляет health-чек "postgres" в /ready
	if err != nil {
		return err
	}

	store := postgresadapter.New(pool)
	if err = registerPrivacy(app, store); err != nil {
		return err
	}
	if executionMetrics != nil {
		app.AddRunnable("execution-queue-metrics", func(ctx context.Context) error {
			return executionMetrics.SampleQueue(ctx, store)
		})
	}

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
	if cfg.LearningOutcomes {
		submissionService = usecase.NewSubmissionServiceWithOutcomes(store)
	}
	languageVersions := map[domain.ProgrammingLanguage]string{
		domain.ProgrammingLanguagePython: cfg.PythonVersion,
		domain.ProgrammingLanguageGo:     cfg.GoVersion,
		domain.ProgrammingLanguageSQLite: cfg.SQLiteVersion,
	}
	topics, err := topiccatalog.New(os.Getenv("TASKS_ENTITIES_URL"))
	if err != nil {
		return err
	}
	taskService.WithPublicationChecks(topics, languageVersions)
	codeSubmissionService := usecase.NewCodeSubmissionService(store, usecase.CodeExecutionPolicy{
		MaxPendingPerUser: cfg.AdmissionMaxPerUser, MaxPendingTotal: cfg.AdmissionMaxTotal, AdmissionRetrySeconds: cfg.AdmissionRetrySeconds,
		RequestsTopic:    cfg.KafkaRequestsTopic,
		LearningOutcomes: cfg.LearningOutcomes,
		TimeLimit:        cfg.CodeExecutionTimeout,
		MemoryLimitBytes: cfg.CodeExecutionMemory,
		MaxAttempts:      cfg.ExecutionMaxAttempts,
		RetryGrace:       cfg.ExecutionRetryGrace,
		PIDs:             cfg.ExecutionPIDs,
		StdoutBytes:      cfg.ExecutionStdoutBytes,
		StderrBytes:      cfg.ExecutionStderrBytes,
		WorkspaceBytes:   cfg.ExecutionWorkspaceBytes,
		LanguageVersions: languageVersions,
	}).WithExecutionMetrics(executionMetrics)
	candidateService := usecase.NewCandidateService(store)
	progressService := usecase.NewProgressService(store)
	problemListService := usecase.NewProblemListService(store)

	httpapi.Register(
		app.HTTP(),
		taskService,
		submissionService,
		codeSubmissionService,
		candidateService,
		problemListService,
		app.Logger(),
		cfg.TaskHunterIngestToken,
	)
	httpapi.RegisterProgress(app.HTTP(), progressService, app.Logger())
	if registerErr := httpapi.RegisterResources(app.HTTP(), usecase.NewResourceService(store, languageVersions), cfg.ResourceToken, app.Logger()); registerErr != nil {
		return registerErr
	}
	if registerErr := httpapi.RegisterTaskActivity(app.HTTP(), usecase.NewTaskActivityService(store), cfg.ActivityToken, app.Logger()); registerErr != nil {
		return registerErr
	}
	var suiteObjects usecase.SuiteObjectStore
	if os.Getenv("TASKS_SUITE_S3_ENDPOINT") != "" || os.Getenv("TASKS_SUITE_S3_BUCKET") != "" || os.Getenv("TASKS_SUITE_S3_ACCESS_KEY_ID") != "" || os.Getenv("TASKS_SUITE_S3_SECRET_ACCESS_KEY") != "" {
		suiteObjects, err = suitestore.New(suitestore.Config{Endpoint: os.Getenv("TASKS_SUITE_S3_ENDPOINT"), Region: os.Getenv("TASKS_SUITE_S3_REGION"), Bucket: os.Getenv("TASKS_SUITE_S3_BUCKET"), AccessKey: os.Getenv("TASKS_SUITE_S3_ACCESS_KEY_ID"), SecretKey: os.Getenv("TASKS_SUITE_S3_SECRET_ACCESS_KEY"), Prefix: os.Getenv("TASKS_SUITE_S3_PREFIX")})
		if err != nil {
			return err
		}
	}
	httpapi.RegisterSuites(app.HTTP(), usecase.NewSuiteService(store, suiteObjects, languageVersions), app.Logger())

	dispatcher := kafkaadapter.NewOutboxDispatcher(store, producer, cfg.OutboxPollInterval, app.Logger()).WithExecutionMetrics(executionMetrics)
	consumer := kafkaadapter.NewResultConsumer(consumerClient, codeSubmissionService, app.Logger())

	// Второй диспетчер — business-события задач (task.published) через стандартную
	// parker-таблицу outbox в топик активности ленты.
	activityDispatcher := parker.NewOutboxDispatcher(parker.NewPgOutbox(pool), producer, cfg.OutboxPollInterval, app.Logger())
	app.AddHealthCheck("kafka-activity", parker.HealthCheckFunc(producer.Ping))

	app.AddRunnable("kafka-outbox", dispatcher.Run)
	app.AddRunnable("task-activity-outbox", activityDispatcher.Run)
	if cfg.LearningOutcomes {
		queue := postgresadapter.NewLearningOutcomeOutbox(pool)
		app.AddRunnable("learning-outcome-outbox", kafkaadapter.NewOutboxDispatcher(queue, producer, cfg.OutboxPollInterval, app.Logger()).Run)
	}
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
			logger.ErrorContext(ctx, "execution reaper failed", "retry_after", interval)
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

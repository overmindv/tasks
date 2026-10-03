package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultKafkaBroker    = "kafka:9092"
	defaultRequestTopic   = "code-execution.requests.v1"
	defaultResultTopic    = "code-execution.results.v1"
	defaultResultGroup    = "tasks-code-results-v1"
	defaultExecutionTTL   = time.Second
	defaultMemoryBytes    = 64 * 1024 * 1024
	defaultOutboxPoll     = 500 * time.Millisecond
	defaultExecutionPIDs  = 32
	defaultStdoutBytes    = 32768
	defaultStderrBytes    = 32768
	defaultWorkspaceBytes = 1048576
	defaultPythonVersion  = "3.12"
	defaultGoVersion      = "1.26"
	// defaultExecutionMaxAttempts лимитирует число переотправок запуска в sandbox (включая первичную).
	defaultExecutionMaxAttempts = 3
	// defaultExecutionRetryGrace — окно, после которого reaper считает queued-запуск заброшенным.
	defaultExecutionRetryGrace = 2 * time.Minute
	// defaultExecutionReaperInterval — период опроса reaper'ом заброшенных запусков.
	defaultExecutionReaperInterval = 30 * time.Second
)

type Config struct {
	TaskHunterIngestToken   string
	KafkaBrokers            []string
	KafkaRequestsTopic      string
	KafkaResultsTopic       string
	KafkaResultsGroup       string
	CodeExecutionTimeout    time.Duration
	CodeExecutionMemory     int64
	OutboxPollInterval      time.Duration
	ExecutionPIDs           int
	ExecutionStdoutBytes    int
	ExecutionStderrBytes    int
	ExecutionWorkspaceBytes int
	ExecutionMaxAttempts    int
	ExecutionRetryGrace     time.Duration
	ExecutionReaperInterval time.Duration
	PythonVersion           string
	GoVersion               string
}

// Load читает, нормализует и проверяет бизнес-конфигурацию окружения.
// Инфраструктура (HTTP, Postgres, лог, Kafka-брокеры producer) — на parker.
func Load() (Config, error) {
	executionTimeout, err := envDuration("CODE_EXECUTION_TIME_LIMIT", defaultExecutionTTL)
	if err != nil {
		return Config{}, err
	}
	outboxPollInterval, err := envDuration("KAFKA_OUTBOX_POLL_INTERVAL", defaultOutboxPoll)
	if err != nil {
		return Config{}, err
	}
	memoryBytes, err := envInt64("CODE_EXECUTION_MEMORY_LIMIT_BYTES", defaultMemoryBytes)
	if err != nil {
		return Config{}, err
	}
	pids, err := envInt("CODE_EXECUTION_PIDS", defaultExecutionPIDs)
	if err != nil {
		return Config{}, err
	}
	stdoutBytes, err := envInt("CODE_EXECUTION_STDOUT_BYTES", defaultStdoutBytes)
	if err != nil {
		return Config{}, err
	}
	stderrBytes, err := envInt("CODE_EXECUTION_STDERR_BYTES", defaultStderrBytes)
	if err != nil {
		return Config{}, err
	}
	workspaceBytes, err := envInt("CODE_EXECUTION_WORKSPACE_BYTES", defaultWorkspaceBytes)
	if err != nil {
		return Config{}, err
	}
	maxAttempts, err := envInt("CODE_EXECUTION_MAX_ATTEMPTS", defaultExecutionMaxAttempts)
	if err != nil {
		return Config{}, err
	}
	retryGrace, err := envDuration("CODE_EXECUTION_RETRY_GRACE", defaultExecutionRetryGrace)
	if err != nil {
		return Config{}, err
	}
	reaperInterval, err := envDuration("CODE_EXECUTION_REAPER_INTERVAL", defaultExecutionReaperInterval)
	if err != nil {
		return Config{}, err
	}

	cfg := Config{
		TaskHunterIngestToken:   strings.TrimSpace(os.Getenv("TASK_HUNTER_INGEST_TOKEN")),
		KafkaBrokers:            envList("KAFKA_BOOTSTRAP_SERVERS", defaultKafkaBroker),
		KafkaRequestsTopic:      env("KAFKA_REQUESTS_TOPIC", defaultRequestTopic),
		KafkaResultsTopic:       env("KAFKA_RESULTS_TOPIC", defaultResultTopic),
		KafkaResultsGroup:       env("KAFKA_RESULTS_CONSUMER_GROUP", defaultResultGroup),
		CodeExecutionTimeout:    executionTimeout,
		CodeExecutionMemory:     memoryBytes,
		OutboxPollInterval:      outboxPollInterval,
		ExecutionPIDs:           pids,
		ExecutionStdoutBytes:    stdoutBytes,
		ExecutionStderrBytes:    stderrBytes,
		ExecutionWorkspaceBytes: workspaceBytes,
		ExecutionMaxAttempts:    maxAttempts,
		ExecutionRetryGrace:     retryGrace,
		ExecutionReaperInterval: reaperInterval,
		PythonVersion:           env("CODE_EXECUTION_PYTHON_VERSION", defaultPythonVersion),
		GoVersion:               env("CODE_EXECUTION_GO_VERSION", defaultGoVersion),
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("validate config: %w", err)
	}

	return cfg, nil
}

// Validate проверяет обязательные и ограниченные бизнес-настройки сервиса.
func (c Config) Validate() error {
	if c.TaskHunterIngestToken == "" {
		return errors.New("TASK_HUNTER_INGEST_TOKEN не задан")
	}
	if len(c.KafkaBrokers) == 0 {
		return errors.New("KAFKA_BOOTSTRAP_SERVERS не задан")
	}
	for _, broker := range c.KafkaBrokers {
		if broker == "" || !strings.Contains(broker, ":") {
			return fmt.Errorf("некорректный Kafka broker %q", broker)
		}
	}
	if c.KafkaRequestsTopic == "" || c.KafkaResultsTopic == "" || c.KafkaResultsGroup == "" {
		return errors.New("kafka topics и consumer group обязательны")
	}
	if c.KafkaRequestsTopic == c.KafkaResultsTopic {
		return errors.New("kafka request и result topics должны различаться")
	}
	if c.CodeExecutionTimeout < time.Millisecond || c.CodeExecutionTimeout > time.Minute {
		return errors.New("CODE_EXECUTION_TIME_LIMIT должен быть от 1ms до 1m")
	}
	if c.CodeExecutionMemory < 1024*1024 || c.CodeExecutionMemory > 4*1024*1024*1024 {
		return errors.New("CODE_EXECUTION_MEMORY_LIMIT_BYTES должен быть от 1 MiB до 4 GiB")
	}
	if c.OutboxPollInterval <= 0 || c.OutboxPollInterval > time.Minute {
		return errors.New("KAFKA_OUTBOX_POLL_INTERVAL должен быть больше нуля и не превышать 1m")
	}
	if c.ExecutionPIDs <= 0 {
		return errors.New("CODE_EXECUTION_PIDS должен быть положительным")
	}
	if c.ExecutionStdoutBytes < 0 || c.ExecutionStderrBytes < 0 || c.ExecutionWorkspaceBytes <= 0 {
		return errors.New("лимиты размера вывода должны быть неотрицательными, workspace — положительным")
	}
	if c.PythonVersion == "" || c.GoVersion == "" {
		return errors.New("версии рантайма python и go не должны быть пустыми")
	}
	if c.ExecutionMaxAttempts < 1 {
		return errors.New("CODE_EXECUTION_MAX_ATTEMPTS должен быть не меньше 1")
	}
	if c.ExecutionRetryGrace <= 0 {
		return errors.New("CODE_EXECUTION_RETRY_GRACE должен быть положительным")
	}
	if c.ExecutionReaperInterval <= 0 {
		return errors.New("CODE_EXECUTION_REAPER_INTERVAL должен быть положительным")
	}

	return nil
}

// env возвращает очищенную переменную окружения или fallback.
func env(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}

	return value
}

// envDuration разбирает duration из окружения или возвращает fallback.
func envDuration(key string, fallback time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}

	duration, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("%s должен быть duration, например 10s или 1m: %w", key, err)
	}

	return duration, nil
}

// envInt разбирает int из окружения или возвращает fallback.
func envInt(key string, fallback int) (int, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}
	number, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s должен быть целым числом: %w", key, err)
	}

	return number, nil
}

// envInt64 разбирает int64 из окружения или возвращает fallback.
func envInt64(key string, fallback int64) (int64, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}
	number, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s должен быть целым числом: %w", key, err)
	}

	return number, nil
}

// envList разбирает непустой comma-separated список без дубликатов.
func envList(key, fallback string) []string {
	value := env(key, fallback)
	items := make([]string, 0)
	seen := make(map[string]struct{})
	for _, item := range strings.Split(value, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		items = append(items, item)
	}

	return items
}

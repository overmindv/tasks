package config

import (
	"testing"
	"time"
)

// TestConfigValidate проверяет обязательные Kafka topics и безопасные лимиты запуска.
func TestConfigValidate(t *testing.T) {
	t.Parallel()
	valid := Config{
		AdmissionMaxPerUser: 3, AdmissionMaxTotal: 1000, AdmissionRetrySeconds: 15,
		TaskHunterIngestToken:   "test-token",
		KafkaBrokers:            []string{"kafka:9092"},
		KafkaRequestsTopic:      "code-execution.requests.v1",
		KafkaResultsTopic:       "code-execution.results.v1",
		KafkaResultsGroup:       "tasks-code-results-v1",
		CodeExecutionTimeout:    time.Second,
		CodeExecutionMemory:     64 * 1024 * 1024,
		OutboxPollInterval:      500 * time.Millisecond,
		ExecutionPIDs:           32,
		ExecutionStdoutBytes:    32768,
		ExecutionStderrBytes:    32768,
		ExecutionWorkspaceBytes: 1048576,
		ExecutionMaxAttempts:    3,
		ExecutionRetryGrace:     time.Minute,
		ExecutionReaperInterval: 30 * time.Second,
		PythonVersion:           "3.12",
		GoVersion:               "1.26",
		SQLiteVersion:           "3.46",
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	invalid := valid
	invalid.KafkaResultsTopic = invalid.KafkaRequestsTopic
	if err := invalid.Validate(); err == nil {
		t.Fatal("Validate() должен отклонить одинаковые request/result topics")
	}
	invalid = valid
	invalid.CodeExecutionTimeout = 500 * time.Microsecond
	if err := invalid.Validate(); err == nil {
		t.Fatal("Validate() должен отклонить timeout меньше 1ms")
	}
}

// TestEnvListNormalizesBrokers проверяет очистку и дедупликацию comma-separated списка.
//
//nolint:paralleltest // t.Setenv изменяет process-wide environment
func TestEnvListNormalizesBrokers(t *testing.T) {
	t.Setenv("TEST_KAFKA_BROKERS", " kafka-a:9092, kafka-b:9092,kafka-a:9092 ")
	items := envList("TEST_KAFKA_BROKERS", "unused:9092")
	if len(items) != 2 || items[0] != "kafka-a:9092" || items[1] != "kafka-b:9092" {
		t.Fatalf("envList() = %#v", items)
	}
}

//nolint:paralleltest // t.Setenv изменяет process-wide environment и subtests выполняются последовательно
func TestAdmissionConfigBounds(t *testing.T) {
	t.Setenv("TASK_HUNTER_INGEST_TOKEN", "test")
	for _, key := range []string{"CODE_ADMISSION_MAX_PER_USER", "CODE_ADMISSION_MAX_TOTAL", "CODE_ADMISSION_RETRY_SECONDS"} {
		t.Run(key, func(t *testing.T) {
			t.Setenv(key, "0")
			if _, err := Load(); err == nil {
				t.Fatal("zero admission policy accepted")
			}
		})
	}
	t.Setenv("CODE_ADMISSION_MAX_PER_USER", "4")
	t.Setenv("CODE_ADMISSION_MAX_TOTAL", "200")
	t.Setenv("CODE_ADMISSION_RETRY_SECONDS", "20")
	cfg, err := Load()
	if err != nil || cfg.AdmissionMaxPerUser != 4 || cfg.AdmissionMaxTotal != 200 || cfg.AdmissionRetrySeconds != 20 {
		t.Fatalf("policy not loaded: %v", err)
	}
}

//nolint:paralleltest // t.Setenv изменяет process-wide environment
func TestLearningOutcomeConfig(t *testing.T) {
	t.Setenv("TASK_HUNTER_INGEST_TOKEN", "test")
	t.Setenv("TASKS_LEARNING_OUTCOMES_ENABLED", "")
	cfg, err := Load()
	if err != nil || cfg.LearningOutcomes {
		t.Fatal("learning outcomes must be disabled by default:", err)
	}
	t.Setenv("TASKS_LEARNING_OUTCOMES_ENABLED", "yes")
	if _, loadErr := Load(); loadErr == nil {
		t.Fatal("invalid learning outcome setting accepted")
	}
	t.Setenv("TASKS_LEARNING_OUTCOMES_ENABLED", "true")
	cfg, err = Load()
	if err != nil || !cfg.LearningOutcomes {
		t.Fatal("learning outcome setting not loaded:", err)
	}
	t.Setenv("KAFKA_RESULTS_TOPIC", "learning.outcome.v1")
	if _, err := Load(); err == nil {
		t.Fatal("learning topic accepted as the execution result topic")
	}
}

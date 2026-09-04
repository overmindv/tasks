package config

import (
	"testing"
	"time"
)

// TestConfigValidate проверяет обязательные Kafka topics и безопасные лимиты запуска.
func TestConfigValidate(t *testing.T) {
	t.Parallel()
	valid := Config{
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
		PythonVersion:           "3.12",
		GoVersion:               "1.26",
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
func TestEnvListNormalizesBrokers(t *testing.T) {
	t.Setenv("TEST_KAFKA_BROKERS", " kafka-a:9092, kafka-b:9092,kafka-a:9092 ")
	items := envList("TEST_KAFKA_BROKERS", "unused:9092")
	if len(items) != 2 || items[0] != "kafka-a:9092" || items[1] != "kafka-b:9092" {
		t.Fatalf("envList() = %#v", items)
	}
}

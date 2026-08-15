package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
)

type pingStub struct {
	err error
}

// Ping возвращает настроенный результат проверки зависимости.
func (p pingStub) Ping(_ context.Context) error {
	return p.err
}

// TestHealthChecksRequirePostgresAndKafka проверяет composite readiness.
func TestHealthChecksRequirePostgresAndKafka(t *testing.T) {
	t.Parallel()
	checks := healthChecks{
		postgres: pingStub{},
		kafka:    pingStub{},
	}
	if err := checks.Ping(context.Background()); err != nil {
		t.Fatalf("Ping() error = %v", err)
	}
	checks.kafka = pingStub{err: errors.New("broker unavailable")}
	if err := checks.Ping(context.Background()); err == nil || !strings.Contains(err.Error(), "kafka") {
		t.Fatalf("Ping() error = %v", err)
	}
}

// TestRunWorkerStopsProcessOnUnexpectedError проверяет fail-fast фонового worker'а.
func TestRunWorkerStopsProcessOnUnexpectedError(t *testing.T) {
	t.Parallel()
	stopped := false
	runWorker(
		context.Background(),
		"test worker",
		func(context.Context) error { return errors.New("unexpected") },
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		func() { stopped = true },
	)
	if !stopped {
		t.Fatal("runWorker() должен остановить process context")
	}
}

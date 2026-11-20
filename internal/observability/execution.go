package observability

import (
	"context"
	"time"

	"github.com/overmindv/tasks/internal/domain"
	"github.com/overmindv/tasks/internal/execution"
	"github.com/prometheus/client_golang/prometheus"
)

const unknownLabel = "unknown"

type AttemptKind uint8

const (
	InitialAttempt AttemptKind = iota + 1
	RetryAttempt
	ReplayAttempt
)

type Execution struct {
	registry *prometheus.Registry

	attempts  [3]prometheus.Counter
	rejected  *prometheus.CounterVec
	verdicts  *prometheus.CounterVec
	latency   *prometheus.HistogramVec
	publish   [2]prometheus.Counter
	outboxAge prometheus.Gauge
	resultAge prometheus.Gauge
	sampleOK  prometheus.Gauge
}

func NewExecution() *Execution {
	counterOpts := func(name, help string) prometheus.CounterOpts {
		return prometheus.CounterOpts{Namespace: "tasks", Subsystem: "execution", Name: name, Help: help}
	}
	gaugeOpts := func(name, help string) prometheus.GaugeOpts {
		return prometheus.GaugeOpts{Namespace: "tasks", Subsystem: "execution", Name: name, Help: help}
	}
	attempts := prometheus.NewCounterVec(counterOpts("attempts_total", "Execution attempts committed with a request in outbox."), []string{"kind"})
	rejected := prometheus.NewCounterVec(counterOpts("rejected_results_total", "Rejected result events recorded in the inbox."), []string{"reason"})
	verdicts := prometheus.NewCounterVec(counterOpts("verdicts_total", "Committed terminal verdicts, including completions after replay."), []string{"run_mode", "verdict"})
	latency := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "tasks", Subsystem: "execution", Name: "final_latency_seconds",
		Help:    "Time from the original submission to a committed terminal verdict.",
		Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600, 1800, 3600},
	}, []string{"run_mode"})
	publish := prometheus.NewCounterVec(counterOpts("publish_attempts_total", "Execution request publish calls, including retries."), []string{"outcome"})
	queue := prometheus.NewGaugeVec(gaugeOpts("queue_oldest_seconds", "Age of the oldest outstanding item in the last successful sample."), []string{"queue"})
	sampleOK := prometheus.NewGauge(gaugeOpts("queue_sample_ok", "Whether the latest execution queue sample succeeded."))
	registry := prometheus.NewRegistry()
	registry.MustRegister(attempts, rejected, verdicts, latency, publish, queue, sampleOK)

	return &Execution{
		registry:  registry,
		attempts:  [3]prometheus.Counter{attempts.WithLabelValues("initial"), attempts.WithLabelValues("retry"), attempts.WithLabelValues("replay")},
		rejected:  rejected,
		verdicts:  verdicts,
		latency:   latency,
		publish:   [2]prometheus.Counter{publish.WithLabelValues("error"), publish.WithLabelValues("ack")},
		outboxAge: queue.WithLabelValues("outbox"),
		resultAge: queue.WithLabelValues("result"),
		sampleOK:  sampleOK,
	}
}

func (m *Execution) Attempt(kind AttemptKind) {
	if m != nil && kind >= InitialAttempt && kind <= ReplayAttempt {
		m.attempts[kind-1].Inc()
	}
}

func (m *Execution) Rejected(code string) {
	if m == nil {
		return
	}
	switch code {
	case execution.InboxErrorInvalidEvent:
		m.rejected.WithLabelValues("invalid_event").Inc()
	case execution.InboxErrorUnsupportedEvent:
		m.rejected.WithLabelValues("unsupported_event").Inc()
	default:
		m.rejected.WithLabelValues("unknown").Inc()
	}
}

func (m *Execution) Final(mode domain.RunMode, verdict domain.ExecutionVerdict, elapsed time.Duration) {
	if m == nil {
		return
	}
	label := unknownLabel
	switch mode {
	case domain.RunModeSamples, domain.RunModeGraded:
		label = string(mode)
	}
	m.verdicts.WithLabelValues(label, verdictLabel(verdict)).Inc()
	m.latency.WithLabelValues(label).Observe(max(0, elapsed.Seconds()))
}

func verdictLabel(verdict domain.ExecutionVerdict) string {
	switch verdict {
	case domain.ExecutionVerdictAccepted, domain.ExecutionVerdictWrongAnswer,
		domain.ExecutionVerdictCompilationError, domain.ExecutionVerdictRuntimeError,
		domain.ExecutionVerdictTimeLimitExceeded, domain.ExecutionVerdictMemoryLimitExceeded,
		domain.ExecutionVerdictOutputLimitExceeded, domain.ExecutionVerdictCheckerError,
		domain.ExecutionVerdictInfrastructureError, domain.ExecutionVerdictCancelled:
		return string(verdict)
	default:
		return "unknown"
	}
}

func (m *Execution) Publish(acked bool) {
	if m == nil {
		return
	}
	if acked {
		m.publish[1].Inc()
	} else {
		m.publish[0].Inc()
	}
}

type QueueReader interface {
	ExecutionQueueAges(context.Context) (outbox, result float64, err error)
}

// SampleQueue runs outside the scrape path so a slow database cannot stall Prometheus.
func (m *Execution) SampleQueue(ctx context.Context, store QueueReader) error {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for ctx.Err() == nil {
		m.sampleQueue(ctx, store)
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}

	return nil
}

func (m *Execution) sampleQueue(ctx context.Context, store QueueReader) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	outbox, result, err := store.ExecutionQueueAges(ctx)
	if err != nil {
		m.sampleOK.Set(0)

		return
	}
	m.outboxAge.Set(outbox)
	m.resultAge.Set(result)
	m.sampleOK.Set(1)
}

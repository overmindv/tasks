package observability

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

var testToken = strings.Repeat("x", 32)

func TestExecutionMetricsRejectUnauthorizedScrapes(t *testing.T) {
	t.Parallel()

	metrics := NewExecution()
	for _, token := range []string{"", "short", strings.Repeat("x", 257), testToken + "\n", testToken + "ж"} {
		if _, err := metrics.Handler(token); err == nil {
			t.Error("invalid token accepted")
		}
	}
	handler, err := metrics.Handler(testToken)
	if err != nil {
		t.Fatal(err)
	}
	for _, headers := range [][]string{nil, {"Basic " + testToken}, {"Bearer " + strings.Repeat("z", 32)}, {"Bearer " + testToken, "Bearer " + testToken}} {
		req := httptest.NewRequest(http.MethodGet, ExecutionMetricsPath, nil)
		for _, header := range headers {
			req.Header.Add("Authorization", header)
		}
		res := httptest.NewRecorder()
		handler.ServeHTTP(res, req)
		if res.Code != http.StatusUnauthorized || res.Header().Get("Cache-Control") != "no-store" ||
			strings.Contains(res.Body.String(), "tasks_execution") {
			t.Errorf("unexpected response: %d %s", res.Code, res.Body)
		}
	}
}

func TestExecutionMetricsAllowlist(t *testing.T) {
	t.Parallel()

	metrics := NewExecution()
	metrics.Final("private-mode", "private-verdict", -time.Second)
	metrics.Rejected("private-reason")
	handler, err := metrics.Handler(testToken)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, ExecutionMetricsPath, nil)
	req.Header.Set("Authorization", "Bearer "+testToken)
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)

	body := res.Body.String()
	if res.Code != http.StatusOK || strings.Contains(body, "private-") {
		t.Errorf("unexpected response: %d %s", res.Code, body)
	}
	for _, line := range []string{
		`tasks_execution_verdicts_total{run_mode="unknown",verdict="unknown"} 1`,
		`tasks_execution_rejected_results_total{reason="unknown"} 1`,
		`tasks_execution_final_latency_seconds_sum{run_mode="unknown"} 0`,
		`tasks_execution_final_latency_seconds_count{run_mode="unknown"} 1`,
	} {
		if !strings.Contains(body, line+"\n") {
			t.Errorf("missing sample: %s", line)
		}
	}
}

type queueSource func(context.Context) (float64, float64, error)

func (f queueSource) ExecutionQueueAges(ctx context.Context) (float64, float64, error) {
	return f(ctx)
}

func TestFailedQueueSampleKeepsLastGoodAges(t *testing.T) {
	t.Parallel()

	m := NewExecution()
	m.sampleQueue(t.Context(), queueSource(func(ctx context.Context) (float64, float64, error) {
		if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > 2*time.Second {
			t.Error("queue sample missing a deadline")
		}
		return 2, 7, nil
	}))
	m.sampleQueue(t.Context(), queueSource(func(context.Context) (float64, float64, error) {
		return 0, 0, errors.New("database unavailable")
	}))

	handler, err := m.Handler(testToken)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, ExecutionMetricsPath, nil)
	req.Header.Set("Authorization", "Bearer "+testToken)
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	for _, line := range []string{
		`tasks_execution_queue_sample_ok 0`,
		`tasks_execution_queue_oldest_seconds{queue="outbox"} 2`,
		`tasks_execution_queue_oldest_seconds{queue="result"} 7`,
	} {
		if !strings.Contains(res.Body.String(), line+"\n") {
			t.Errorf("missing sample: %s", line)
		}
	}
}

func BenchmarkRecordExecution(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		name := "disabled"
		var metrics *Execution
		if enabled {
			name = "enabled"
			metrics = NewExecution()
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				metrics.Attempt(InitialAttempt)
				metrics.Final("samples", "accepted", 50*time.Millisecond)
			}
		})
	}
}

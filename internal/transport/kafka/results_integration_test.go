//go:build integration

package kafka

import (
	"context"
	"errors"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/execution"
	"github.com/overmindv/tasks/internal/usecase"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestResultConsumerKafka проверяет реальные offsets и передачу partition другому
// consumer. Ошибка хранилища инъецируется на границе HandleRunResult.
func TestResultConsumerKafka(t *testing.T) {
	brokers := os.Getenv("TEST_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("TEST_KAFKA_BROKERS is required")
	}
	for _, mode := range []string{"transient", "restart", "rebalance"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			topic := "ovm255-" + uuid.NewString()
			group := topic + "-group"
			admin, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(admin.Close)
			req := kmsg.NewPtrCreateTopicsRequest()
			req.Topics = []kmsg.CreateTopicsRequestTopic{{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}}
			response, err := req.RequestWith(ctx, admin)
			if err != nil {
				t.Fatal(err)
			}
			if response.Topics[0].ErrorCode != 0 {
				t.Fatalf("create topic error: %d", response.Topics[0].ErrorCode)
			}
			t.Cleanup(func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cleanupCancel()
				deleteReq := kmsg.NewPtrDeleteTopicsRequest()
				deleteReq.TopicNames = []string{topic}
				if _, err := deleteReq.RequestWith(cleanupCtx, admin); err != nil {
					t.Logf("topic cleanup: %v", err)
				}
			})
			records := resultRecords()
			for _, r := range records {
				r.Topic = topic
				r.Key = []byte(uuid.NewString())
			}
			if err := admin.ProduceSync(ctx, records...).FirstErr(); err != nil {
				t.Fatal(err)
			}
			first, err := NewConsumerClient(strings.Split(brokers, ","), topic, group)
			if err != nil {
				t.Fatal(err)
			}
			defer first.CloseAllowingRebalance()
			handler := &integrationResultHandler{failed: make(chan struct{})}
			if mode == "transient" {
				handler.failures.Store(1)
			} else {
				handler.failures.Store(100)
			}
			firstConsumer := &ResultConsumer{client: first, service: handler, logger: discardLogger()}
			done := make(chan error, 1)
			budget := 200 * time.Millisecond
			if mode == "transient" {
				budget = resultRetryBudget
			}
			go func() { done <- firstConsumer.run(ctx, budget) }()
			select {
			case <-handler.failed:
			case <-ctx.Done():
				t.Fatal("first consumer did not receive record")
			}
			var second *kgo.Client
			var secondDone chan error
			if mode == "rebalance" {
				second, err = NewConsumerClient(strings.Split(brokers, ","), topic, group)
				if err != nil {
					t.Fatal(err)
				}
				defer second.CloseAllowingRebalance()
				recovered := &integrationResultHandler{failed: make(chan struct{})}
				secondConsumer := &ResultConsumer{client: second, service: recovered, logger: discardLogger()}
				secondDone = make(chan error, 1)
				go func() { secondDone <- secondConsumer.Run(ctx) }()
				// Первый consumer ещё удерживает N; второй инициирует вступление в группу.
				select {
				case err := <-done:
					if !errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("first consumer = %v", err)
					}
				case <-ctx.Done():
					t.Fatal("first consumer did not exhaust budget")
				}
				first.CloseAllowingRebalance()
				awaitCommitted(t, ctx, admin, group, topic, 2)
				cancel()
				if err := <-secondDone; err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(recovered.saved, []int64{0, 1}) {
					t.Fatalf("rebalance saved = %v", recovered.saved)
				}
				return
			}
			if mode == "restart" {
				select {
				case err := <-done:
					if !errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("first consumer = %v", err)
					}
				case <-ctx.Done():
					t.Fatal("first consumer did not exhaust budget")
				}
				first.CloseAllowingRebalance()
				second, err = NewConsumerClient(strings.Split(brokers, ","), topic, group)
				if err != nil {
					t.Fatal(err)
				}
				defer second.CloseAllowingRebalance()
				recovered := &integrationResultHandler{failed: make(chan struct{})}
				secondConsumer := &ResultConsumer{client: second, service: recovered, logger: discardLogger()}
				secondDone = make(chan error, 1)
				go func() { secondDone <- secondConsumer.Run(ctx) }()
				awaitCommitted(t, ctx, admin, group, topic, 2)
				cancel()
				if err := <-secondDone; err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(recovered.saved, []int64{0, 1}) {
					t.Fatalf("restart saved = %v", recovered.saved)
				}
				return
			}
			awaitCommitted(t, ctx, admin, group, topic, 2)
			cancel()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(handler.saved, []int64{0, 1}) {
				t.Fatalf("transient saved = %v", handler.saved)
			}
		})
	}
}

type integrationResultHandler struct {
	fakeResultHandler
	failures atomic.Int32
	failed   chan struct{}
	saved    []int64
}

func (h *integrationResultHandler) HandleRunResult(_ context.Context, m usecase.ExecutionMessageMetadata, _ execution.RunResult) error {
	if h.failures.Add(-1) >= 0 {
		select {
		case <-h.failed:
		default:
			close(h.failed)
		}
		return errors.New("database unavailable")
	}
	h.saved = append(h.saved, m.Offset)
	return nil
}

func awaitCommitted(t *testing.T, ctx context.Context, client *kgo.Client, group, topic string, want int64) {
	t.Helper()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		req := kmsg.NewPtrOffsetFetchRequest()
		// v7 использует Group/Topics без нового batch group API.
		req.SetVersion(7)
		req.Group = group
		req.Topics = []kmsg.OffsetFetchRequestTopic{{Topic: topic, Partitions: []int32{0}}}
		response, err := req.RequestWith(ctx, client)
		if err == nil {
			for _, entry := range response.Topics {
				for _, partition := range entry.Partitions {
					if partition.ErrorCode == 0 && partition.Offset == want {
						return
					}
				}
			}
			for _, g := range response.Groups {
				for _, entry := range g.Topics {
					for _, partition := range entry.Partitions {
						if partition.ErrorCode == 0 && partition.Offset == want {
							return
						}
					}
				}
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("committed offset did not reach %d: %v", want, err)
		case <-ticker.C:
		}
	}
}

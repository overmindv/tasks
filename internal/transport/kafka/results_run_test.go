package kafka

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/execution"
	"github.com/overmindv/tasks/internal/usecase"
	"github.com/twmb/franz-go/pkg/kgo"
)

// orderedResultClient моделирует последовательную выдачу offset одним partition.
type orderedResultClient struct {
	records          []*kgo.Record
	next             int
	commits          []int64
	commitFailures   int
	blocked          bool
	prematureRelease bool
	pending          bool
	cancel           context.CancelFunc
}

func (f *orderedResultClient) PollRecords(ctx context.Context, _ int) kgo.Fetches {
	if f.next == len(f.records) {
		f.cancel()
		return nil
	}
	r := f.records[f.next]
	f.next++
	f.blocked = true
	f.pending = true
	return kgo.Fetches{{Topics: []kgo.FetchTopic{{Topic: r.Topic, Partitions: []kgo.FetchPartition{{Partition: r.Partition, Records: []*kgo.Record{r}}}}}}}
}
func (f *orderedResultClient) CommitRecords(_ context.Context, rs ...*kgo.Record) error {
	if f.commitFailures > 0 {
		f.commitFailures--
		return errors.New("coordinator unavailable")
	}
	f.commits = append(f.commits, rs[0].Offset)
	f.pending = false
	return nil
}
func (f *orderedResultClient) AllowRebalance() {
	if f.pending {
		f.prematureRelease = true
	}
	f.blocked = false
}

type retryResultHandler struct {
	fakeResultHandler
	failures int
	offsets  []int64
	saved    []int64
}

func (f *retryResultHandler) HandleRunResult(_ context.Context, m usecase.ExecutionMessageMetadata, _ execution.RunResult) error {
	f.offsets = append(f.offsets, m.Offset)
	if f.failures > 0 {
		f.failures--
		return errors.New("database unavailable")
	}
	f.saved = append(f.saved, m.Offset)
	return nil
}

func resultRecords() []*kgo.Record {
	return []*kgo.Record{
		{Topic: "results", Offset: 10, Value: validResultPayload(uuid.NewString(), uuid.NewString())},
		{Topic: "results", Offset: 11, Value: validResultPayload(uuid.NewString(), uuid.NewString())},
	}
}

func TestResultConsumerRetriesBeforePollingAhead(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		dbFailures, commitFailures int
		wantHandled                []int64
	}{
		{"database failure", 1, 0, []int64{10, 10, 11}},
		{"commit failure", 0, 1, []int64{10, 11}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			client := &orderedResultClient{records: resultRecords(), cancel: cancel, commitFailures: tc.commitFailures}
			handler := &retryResultHandler{failures: tc.dbFailures}
			consumer := &ResultConsumer{client: client, service: handler, logger: discardLogger()}
			if err := consumer.Run(ctx); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(handler.offsets, tc.wantHandled) {
				t.Errorf("handled offsets = %v, want %v", handler.offsets, tc.wantHandled)
			}
			if !reflect.DeepEqual(client.commits, []int64{10, 11}) {
				t.Errorf("committed offsets = %v, want [10 11]", client.commits)
			}
			if client.prematureRelease {
				t.Error("rebalance allowed before current record was committed")
			}
		})
	}
}

func TestResultConsumerExhaustionReleasesForRedelivery(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		dbFailures, commitFailures int
	}{
		{"database unavailable", 100, 0},
		{"commit unavailable", 0, 100},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			client := &orderedResultClient{records: resultRecords(), cancel: cancel, commitFailures: tc.commitFailures}
			handler := &retryResultHandler{failures: tc.dbFailures}
			consumer := &ResultConsumer{client: client, service: handler, logger: discardLogger()}
			err := consumer.run(ctx, 10*time.Millisecond)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("Run error = %v, want deadline exceeded", err)
			}
			if client.next != 1 || len(client.commits) != 0 || client.blocked {
				t.Fatalf("unsafe exit: polled=%d commits=%v blocked=%v", client.next, client.commits, client.blocked)
			}
			// Новый владелец начинает с последнего подтверждённого offset. Это модель
			// повторной доставки; настоящий rebalance проверяется интеграционно.
			replay := &orderedResultClient{records: client.records, cancel: cancel}
			recovered := &retryResultHandler{}
			nextConsumer := &ResultConsumer{client: replay, service: recovered, logger: discardLogger()}
			if err := nextConsumer.Run(ctx); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(recovered.saved, []int64{10, 11}) || !reflect.DeepEqual(replay.commits, []int64{10, 11}) {
				t.Fatalf("redelivery lost records: saved=%v commits=%v", recovered.saved, replay.commits)
			}
		})
	}
}

func TestResultConsumerCancellationDuringRetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	client := &orderedResultClient{records: resultRecords(), cancel: cancel}
	consumer := &ResultConsumer{client: client, service: &retryResultHandler{failures: 100}, logger: discardLogger()}
	if err := consumer.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if client.next != 1 || len(client.commits) != 0 || client.blocked {
		t.Fatalf("unsafe cancellation: polled=%d commits=%v blocked=%v", client.next, client.commits, client.blocked)
	}
}

func TestResultConsumerRetriesRejectedInbox(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	records := resultRecords()
	records[0].Value = []byte(`invalid-json`)
	client := &orderedResultClient{records: records, cancel: cancel}
	handler := &retryRejectHandler{}
	consumer := &ResultConsumer{client: client, service: handler, logger: discardLogger()}
	if err := consumer.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if handler.rejectAttempts != 2 || !reflect.DeepEqual(client.commits, []int64{10, 11}) || client.prematureRelease {
		t.Fatalf("reject attempts=%d commits=%v premature release=%v", handler.rejectAttempts, client.commits, client.prematureRelease)
	}
}

type retryRejectHandler struct {
	fakeResultHandler
	rejectAttempts int
}

func (f *retryRejectHandler) RejectResult(context.Context, usecase.ExecutionMessageMetadata, *uuid.UUID, string) error {
	f.rejectAttempts++
	if f.rejectAttempts == 1 {
		return errors.New("database unavailable")
	}
	return nil
}

// Kafka может выдать ошибку одного partition вместе с записью другого.
type mixedFetchClient struct{ orderedResultClient }

func (f *mixedFetchClient) PollRecords(ctx context.Context, limit int) kgo.Fetches {
	fetches := f.orderedResultClient.PollRecords(ctx, limit)
	if len(fetches) > 0 {
		fetches[0].Topics[0].Partitions = append(fetches[0].Topics[0].Partitions, kgo.FetchPartition{Partition: 1, Err: errors.New("partition unavailable")})
	}
	return fetches
}
func TestResultConsumerPreservesRecordsAlongsideFetchErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &mixedFetchClient{orderedResultClient{records: resultRecords(), cancel: cancel}}
	handler := &retryResultHandler{}
	consumer := &ResultConsumer{client: client, service: handler, logger: discardLogger()}
	if err := consumer.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(handler.saved, []int64{10, 11}) || !reflect.DeepEqual(client.commits, []int64{10, 11}) {
		t.Fatalf("lost mixed fetch: saved=%v commits=%v", handler.saved, client.commits)
	}
}

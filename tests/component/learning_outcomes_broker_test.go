//go:build component

package component

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/overmindv/parker"
	postgresadapter "github.com/overmindv/tasks/internal/adapter/postgres"
	"github.com/overmindv/tasks/internal/domain"
	kafkaadapter "github.com/overmindv/tasks/internal/transport/kafka"
	"github.com/overmindv/tasks/internal/usecase"
	"github.com/twmb/franz-go/pkg/kgo"
)

type loseFirstAck struct {
	kafkaadapter.Publisher
	failed bool
}

func (p *loseFirstAck) Publish(ctx context.Context, topic, key string, payload []byte) error {
	if err := p.Publisher.Publish(ctx, topic, key, payload); err != nil {
		return err
	}
	if !p.failed {
		p.failed = true
		return errors.New("broker acknowledgement lost")
	}
	return nil
}

func TestLearningOutcomeBrokerRetry(t *testing.T) {
	brokers := os.Getenv("OUTCOME_TEST_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("OUTCOME_TEST_KAFKA_BROKERS must point to a dedicated broker")
	}
	pool := openComponentDB(t)
	store := postgresadapter.New(pool)
	topic := uuid.New()
	task := publishOutcomeFixture(t, usecase.NewTaskService(store), uuid.New(),
		domain.TaskInput{TopicID: &topic, Title: "Outcome", Statement: "Choose an answer", TaskType: domain.TaskTypeSingleChoice,
			Options: []domain.OptionInput{{Text: "Wrong"}, {Text: "PRIVATE_OPTION", IsCorrect: true}}})
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	row, err := usecase.NewSubmissionServiceWithOutcomes(store).Submit(ctx, task.Task.ID, uuid.New(),
		usecase.SubmitInput{TaskVersionID: task.Version.ID, IdempotencyKey: uuid.New(), SelectedOptionIDs: []uuid.UUID{task.Version.Options[0].ID}})
	if err != nil {
		t.Fatal(err)
	}
	want := readOutcome(t, pool, row.ID)
	producer, err := parker.NewProducer(strings.Split(brokers, ","))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	consumer, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...),
		kgo.ConsumeTopics(domain.LearningOutcomeTopic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	publisher := &loseFirstAck{Publisher: producer}
	dispatcher := kafkaadapter.NewOutboxDispatcher(postgresadapter.NewLearningOutcomeOutbox(pool), publisher, 25*time.Millisecond,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	done := make(chan error, 1)
	go func() { done <- dispatcher.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	offsets := make(map[int64]struct{}, 2)
	for len(offsets) < 2 && ctx.Err() == nil {
		records := consumer.PollRecords(ctx, 100)
		records.EachRecord(func(record *kgo.Record) {
			if string(record.Key) != row.ID.String() {
				return
			}
			var event domain.LearningOutcome
			if err := json.Unmarshal(record.Value, &event); err != nil || event.EventID != want.EventID ||
				event.Payload.LearnerID != row.UserID || event.Payload.Verdict != domain.ExecutionVerdictWrongAnswer ||
				strings.Contains(string(record.Value), "PRIVATE_OPTION") {
				t.Error("unexpected published outcome:", err)
				return
			}
			offsets[record.Offset] = struct{}{}
		})
	}
	if len(offsets) != 2 {
		t.Fatal("expected two deliveries of the same event ID:", len(offsets), ctx.Err())
	}
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		var cleared, published bool
		var attempts int
		if err := pool.QueryRow(ctx, `
			SELECT payload IS NULL, published_at IS NOT NULL, attempts FROM learning_outcome_outbox WHERE id=$1`, want.EventID).
			Scan(&cleared, &published, &attempts); err != nil {
			t.Fatal(err)
		}
		if published {
			if !cleared || attempts != 1 {
				t.Fatal("outbox did not retry once and clear the payload:", attempts, cleared)
			}
			return
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("outbox did not acknowledge publication:", ctx.Err())
		}
	}
}

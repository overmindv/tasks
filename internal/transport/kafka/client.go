package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Бюджет записи меньше rebalance timeout, чтобы успеть освободить группу.
const resultRetryBudget = 30 * time.Second

// Publisher задаёт минимальный контракт синхронной публикации outbox message
// (реализуется *parker.Producer).
type Publisher interface {
	Publish(ctx context.Context, topic, key string, payload []byte) error
}

// NewConsumerClient создаёт отдельный group consumer с ручным offset commit
// для чтения финальных результатов выполнения кода от sandbox.
func NewConsumerClient(brokers []string, topic, group string) (*kgo.Client, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.RebalanceTimeout(2*resultRetryBudget),
	)
	if err != nil {
		return nil, fmt.Errorf("create Kafka result consumer: %w", err)
	}

	return client, nil
}

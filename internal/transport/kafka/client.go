package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/overmindv/parker"
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
//
// TLS/SASL берутся из parker: без аргументов настройки читаются из окружения
// так же, как у producer, и production без TLS отклоняется до подключения.
func NewConsumerClient(brokers []string, topic, group string, security ...parker.KafkaSecurity) (*kgo.Client, error) {
	securityOptions, err := parker.KafkaClientOptions(security...)
	if err != nil {
		return nil, fmt.Errorf("create Kafka result consumer: %w", err)
	}
	options := append(securityOptions,
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.RebalanceTimeout(2*resultRetryBudget),
	)
	client, err := kgo.NewClient(options...)
	if err != nil {
		return nil, fmt.Errorf("create Kafka result consumer: %w", err)
	}

	return client, nil
}

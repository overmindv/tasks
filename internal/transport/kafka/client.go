package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Publisher задаёт минимальный контракт синхронной публикации outbox message.
type Publisher interface {
	Publish(ctx context.Context, topic, key string, payload []byte) error
}

// Producer публикует подтверждённые Kafka records через franz-go.
type Producer struct {
	client *kgo.Client
}

// NewProducer создаёт idempotent producer для request topic.
func NewProducer(brokers []string) (*Producer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return nil, fmt.Errorf("create Kafka producer: %w", err)
	}

	return &Producer{client: client}, nil
}

// Publish ждёт broker acknowledgement перед подтверждением outbox message.
func (p *Producer) Publish(ctx context.Context, topic, key string, payload []byte) error {
	result := p.client.ProduceSync(ctx, &kgo.Record{
		Topic: topic,
		Key:   []byte(key),
		Value: payload,
	})
	if err := result.FirstErr(); err != nil {
		return fmt.Errorf("produce Kafka record: %w", err)
	}

	return nil
}

// Ping проверяет соединение с Kafka controller.
func (p *Producer) Ping(ctx context.Context) error {
	if err := p.client.Ping(ctx); err != nil {
		return fmt.Errorf("ping Kafka: %w", err)
	}

	return nil
}

// Close завершает producer и освобождает сетевые ресурсы.
func (p *Producer) Close() {
	p.client.Close()
}

// NewConsumerClient создаёт отдельный group consumer с ручным offset commit.
func NewConsumerClient(brokers []string, topic, group string) (*kgo.Client, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
	)
	if err != nil {
		return nil, fmt.Errorf("create Kafka result consumer: %w", err)
	}

	return client, nil
}

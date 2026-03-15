package rabbit

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// HandlerFunc обрабатывает одну задачу из очереди.
type HandlerFunc func(ctx context.Context, task RecalcFeaturesTask) error

// Consumer читает задачи из очереди и передаёт в handler.
type Consumer struct {
	ch       *amqp.Channel
	queue    string
	prefetch int
	handler  HandlerFunc
}

// NewConsumer создаёт consumer для очереди.
func NewConsumer(ch *amqp.Channel, queue string, prefetch int, handler HandlerFunc) (*Consumer, error) {
	if prefetch <= 0 {
		prefetch = 64
	}
	if err := ch.Qos(prefetch, 0, false); err != nil {
		return nil, fmt.Errorf("set qos: %w", err)
	}

	return &Consumer{
		ch:       ch,
		queue:    queue,
		prefetch: prefetch,
		handler:  handler,
	}, nil
}

// Run запускает цикл потребления до закрытия ctx.
func (c *Consumer) Run(ctx context.Context) error {
	deliveries, err := c.ch.Consume(
		c.queue,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("start consume: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case d, ok := <-deliveries:
			if !ok {
				return nil
			}

			var task RecalcFeaturesTask
			if err := json.Unmarshal(d.Body, &task); err != nil {
				_ = d.Nack(false, false)
				continue
			}

			if err := c.handler(ctx, task); err != nil {
				_ = d.Nack(false, false)
				continue
			}

			_ = d.Ack(false)
		}

	}
}

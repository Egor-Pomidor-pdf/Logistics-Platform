package rabbit

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// RecalcFeaturesTask описывает задачу на пересчёт ETA-фич.
type RecalcFeaturesTask struct {
	ShipmentID string `json:"shipment_id"`
	Reason     string `json:"reason,omitempty"`
	Force      bool   `json:"force,omitempty"`
}

// Publisher публикует задачи в RabbitMQ exchange.
type Publisher struct {
	ch       *amqp.Channel
	exchange string
	routing  string
}

// NewPublisher создаёт новый Publisher.
func NewPublisher(ch *amqp.Channel, exchange, routingKey string) *Publisher {
	return &Publisher{
		ch:       ch,
		exchange: exchange,
		routing:  routingKey,
	}
}

// PublishRecalcFeatures публикует задачу на пересчёт фич.
func (p *Publisher) PublishRecalcFeatures(ctx context.Context, task RecalcFeaturesTask) error {
	body, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("marshal task: %w", err)
	}

	if err := p.ch.PublishWithContext(ctx,
		p.exchange,
		p.routing,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	); err != nil {
		return fmt.Errorf("publish recalc_features: %w", err)
	}

	return nil
}


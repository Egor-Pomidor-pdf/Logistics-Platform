package worker

import (
	"context"
	"fmt"
	"time"

	"log/slog"

	"github.com/dns/logistics/eta-data-service/internal/cache"
	"github.com/dns/logistics/eta-data-service/internal/domain"
	"github.com/dns/logistics/eta-data-service/internal/repository"
	"github.com/dns/logistics/eta-data-service/internal/service"
	rabbittransport "github.com/dns/logistics/eta-data-service/internal/transport/rabbit"
	"github.com/rabbitmq/amqp091-go"
)

// RecalcFeaturesProcessor реализует TaskProcessor, обрабатывая задачи из RabbitMQ.
type RecalcFeaturesProcessor struct {
	logger     *slog.Logger
	repo       repository.PostgresStore
	cache      cache.FeaturesCache
	featureTTL time.Duration
	consumer   *rabbittransport.Consumer
}

// NewRecalcFeaturesProcessor создаёт новый процессор.
func NewRecalcFeaturesProcessor(
	logger *slog.Logger,
	repo repository.PostgresStore,
	cache cache.FeaturesCache,
	featureTTL time.Duration,
	consumer *rabbittransport.Consumer,
) *RecalcFeaturesProcessor {
	return &RecalcFeaturesProcessor{
		logger:     logger,
		repo:       repo,
		cache:      cache,
		featureTTL: featureTTL,
		consumer:   consumer,
	}
}

// Run делегирует выполнение consumer.Run(ctx).
func (p *RecalcFeaturesProcessor) Run(ctx context.Context) error {
	handler := func(ctx context.Context, task rabbittransport.RecalcFeaturesTask) error {
		if task.ShipmentID == "" {
			return fmt.Errorf("empty shipment_id in task")
		}

		// Для MVP: берём историю за разумное окно по времени (например, 30 дней назад до сейчас).
		now := time.Now().UTC()
		since := now.AddDate(0, 0, -30)

		history, err := p.repo.ListShipmentHistory(ctx, repository.ShipmentHistoryFilter{
			Since: since,
			Until: now,
			Limit: 10000,
		})
		if err != nil {
			return fmt.Errorf("list history: %w", err)
		}

		// Фильтруем события по нужному shipment_id.
		filtered := make([]service.ShipmentFeatures, 0, 1)
		features := service.BuildFeaturesForShipments(now, history, map[string]domain.ShipmentState{})
		for _, f := range features {
			if f.ShipmentID == task.ShipmentID {
				filtered = append(filtered, f)
			}
		}
		if len(filtered) == 0 {
			return nil
		}

		if err := p.cache.SetShipmentFeatures(ctx, filtered[0], p.featureTTL); err != nil {
			return fmt.Errorf("set features cache: %w", err)
		}

		p.logger.Info("features recomputed and cached",
			"shipment_id", task.ShipmentID,
			"reason", task.Reason,
		)
		return nil
	}

	p.consumer, _ = rabbittransport.NewConsumer(p.consumerChannel(), p.consumerQueue(), 0, handler)
	return p.consumer.Run(ctx)
}

// helper methods to avoid exporting low-level AMQP details from here.
func (p *RecalcFeaturesProcessor) consumerChannel()  *amqp091.Channel {
	return nil
}

func (p *RecalcFeaturesProcessor) consumerQueue() string {
	return ""
}


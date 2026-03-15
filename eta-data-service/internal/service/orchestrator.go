package service

import (
	"context"
	"time"

	"log/slog"

	"go.opentelemetry.io/otel/trace"

	"github.com/dns/logistics/eta-data-service/internal/metrics"
	"github.com/dns/logistics/eta-data-service/internal/repository"
)

// Orchestrator инкапсулирует батчевые сценарии подготовки данных для ML.
type Orchestrator struct {
	logger  *slog.Logger
	repo    repository.PostgresStore
	metrics *metrics.Collector
	tracer  trace.Tracer
}

// NewOrchestrator создаёт оркестратор.
func NewOrchestrator(
	logger *slog.Logger,
	repo repository.PostgresStore,
	metricCollector *metrics.Collector,
	tracer trace.Tracer,
) *Orchestrator {
	return &Orchestrator{
		logger:  logger,
		repo:    repo,
		metrics: metricCollector,
		tracer:  tracer,
	}
}

// RunTrainingBatch подготавливает обучающий батч за указанный интервал и агрегирует фичи.
// На этом этапе метод только строит фичи и логирует объём, не ходя во внешний ML-сервис.
func (o *Orchestrator) RunTrainingBatch(ctx context.Context, since, until time.Time, limit int) ([]ShipmentFeatures, error) {
	ctx, span := o.tracer.Start(ctx, "orchestrator.run_training_batch")
	defer span.End()

	history, err := o.repo.ListShipmentHistory(ctx, repository.ShipmentHistoryFilter{
		Since: since,
		Until: until,
		Limit: limit,
	})
	if err != nil {
		o.metrics.ObserveBatchRun(ctx, "training", "error")
		return nil, err
	}

	// для обучения достаточно истории; текущие состояния опциональны.
	features := BuildFeaturesForShipments(time.Now().UTC(), history, nil)

	o.logger.Info("training batch prepared",
		"events", len(history),
		"shipments", len(features),
		"since", since,
		"until", until,
	)

	o.metrics.ObserveBatchRun(ctx, "training", "ok")
	return features, nil
}

// RunScoringBatch подготавливает батч для скоринга активных отправлений.
func (o *Orchestrator) RunScoringBatch(ctx context.Context, limit, offset int) ([]ShipmentFeatures, error) {
	ctx, span := o.tracer.Start(ctx, "orchestrator.run_scoring_batch")
	defer span.End()

	states, err := o.repo.ListActiveShipmentStates(ctx, repository.ShipmentStateFilter{
		Limit:  limit,
		Offset: offset,
	})
	if err != nil {
		o.metrics.ObserveBatchRun(ctx, "scoring", "error")
		return nil, err
	}

	// Для простоты строим фичи только по статусу/возрасту, без полной истории.
	now := time.Now().UTC()
	features := make([]ShipmentFeatures, 0, len(states))
	for _, st := range states {
		age := int64(now.Sub(st.StatusTS).Seconds())
		if age < 0 {
			age = 0
		}
		features = append(features, ShipmentFeatures{
			ShipmentID:        st.ShipmentID.String(),
			RouteID:           st.RouteID,
			SourceSystem:      st.SourceSystem,
			CurrentStatusCode: st.StatusCode,
			CurrentStatusTS:   st.StatusTS,
			EventsCount:       0,
			AgeSeconds:        age,
		})
	}

	o.logger.Info("scoring batch prepared",
		"shipments", len(features),
		"limit", limit,
		"offset", offset,
	)

	o.metrics.ObserveBatchRun(ctx, "scoring", "ok")
	return features, nil
}


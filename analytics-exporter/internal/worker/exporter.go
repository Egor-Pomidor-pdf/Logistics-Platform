package worker

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/dns/logistics/analytics-exporter/internal/clickhouse"
	"github.com/dns/logistics/analytics-exporter/internal/config"
	analyticsmetrics "github.com/dns/logistics/analytics-exporter/internal/metrics"
	analyticsrepo "github.com/dns/logistics/analytics-exporter/internal/repository"
	"github.com/dns/logistics/analytics-exporter/internal/state"
)

const (
	streamHistory = "shipment_history"
	streamState   = "shipment_state"
)

// Exporter runs incremental export to ClickHouse.
type Exporter struct {
	logger  *slog.Logger
	repo    *analyticsrepo.PostgresRepository
	state   *state.Store
	ch      *clickhouse.Client
	metrics *analyticsmetrics.Collector
	cfg     config.Config
}

// NewExporter creates exporter.
func NewExporter(
	logger *slog.Logger,
	repo *analyticsrepo.PostgresRepository,
	stateStore *state.Store,
	ch *clickhouse.Client,
	metrics *analyticsmetrics.Collector,
	cfg config.Config,
) *Exporter {
	return &Exporter{
		logger:  logger,
		repo:    repo,
		state:   stateStore,
		ch:      ch,
		metrics: metrics,
		cfg:     cfg,
	}
}

// Run starts export loop.
func (e *Exporter) Run(ctx context.Context) error {
	ticker := time.NewTicker(e.cfg.ExportInterval)
	defer ticker.Stop()

	// First run immediately.
	if err := e.RunOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
		e.logger.Error("initial export failed", "error", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := e.RunOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
				e.logger.Error("scheduled export failed", "error", err)
			}
		}
	}
}

// RunOnce runs configured exports once.
func (e *Exporter) RunOnce(ctx context.Context) error {
	if e.cfg.ExportHistory {
		if err := e.runStream(ctx, streamHistory, e.exportHistory); err != nil {
			return err
		}
	}
	if e.cfg.ExportState {
		if err := e.runStream(ctx, streamState, e.exportState); err != nil {
			return err
		}
	}
	return nil
}

func (e *Exporter) runStream(ctx context.Context, stream string, fn func(context.Context) (int, error)) error {
	start := time.Now()
	err := e.state.WithAdvisoryLock(ctx, stream, func(lockCtx context.Context) error {
		rows, runErr := fn(lockCtx)
		result := "ok"
		if runErr != nil {
			result = "error"
		}
		e.metrics.ObserveExport(lockCtx, stream, result, time.Since(start), rows)
		return runErr
	})
	if errors.Is(err, state.ErrLockNotAcquired) {
		e.logger.Debug("export lock not acquired", "stream", stream)
		return nil
	}
	return err
}

func (e *Exporter) exportHistory(ctx context.Context) (int, error) {
	until := time.Now().UTC().Add(-e.cfg.ExportLag)
	if until.Before(time.Time{}) {
		until = time.Now().UTC()
	}

	cursor, ok, err := e.state.GetCursor(ctx, streamHistory)
	if err != nil {
		return 0, err
	}
	if !ok {
		cursor.TS = until.Add(-e.cfg.BackfillWindow)
		cursor.ID = ""
	}

	total := 0
	for {
		items, err := e.repo.ListHistorySince(ctx, cursor.TS, cursor.ID, until, e.cfg.ExportBatch)
		if err != nil {
			return total, err
		}
		if len(items) == 0 {
			break
		}

		last := items[len(items)-1]
		exportedAt := time.Now().UTC()
		if err := e.ch.InsertHistory(ctx, items, exportedAt); err != nil {
			return total, err
		}
		total += len(items)

		cursor = state.Cursor{TS: last.StatusTS, ID: last.EventID}
		if err := e.state.UpdateCursor(ctx, streamHistory, cursor); err != nil {
			return total, err
		}

		if len(items) < e.cfg.ExportBatch {
			break
		}
	}

	e.logger.Info("history export completed", "rows", total, "until", until)
	return total, nil
}

func (e *Exporter) exportState(ctx context.Context) (int, error) {
	cursor, ok, err := e.state.GetCursor(ctx, streamState)
	if err != nil {
		return 0, err
	}
	if !ok {
		cursor.TS = time.Time{}
		cursor.ID = ""
	}

	total := 0
	for {
		items, err := e.repo.ListStatesSince(ctx, cursor.TS, cursor.ID, e.cfg.ExportBatch)
		if err != nil {
			return total, err
		}
		if len(items) == 0 {
			break
		}

		last := items[len(items)-1]
		exportedAt := time.Now().UTC()
		if err := e.ch.InsertStates(ctx, items, exportedAt); err != nil {
			return total, err
		}
		total += len(items)

		cursor = state.Cursor{TS: last.UpdatedAt, ID: last.ShipmentID.String()}
		if err := e.state.UpdateCursor(ctx, streamState, cursor); err != nil {
			return total, err
		}

		if len(items) < e.cfg.ExportBatch {
			break
		}
	}

	e.logger.Info("state export completed", "rows", total)
	return total, nil
}

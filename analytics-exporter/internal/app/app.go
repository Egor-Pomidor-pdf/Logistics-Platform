package app

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/dns/logistics/analytics-exporter/internal/clickhouse"
	"github.com/dns/logistics/analytics-exporter/internal/config"
	"github.com/dns/logistics/analytics-exporter/internal/logging"
	analyticsmetrics "github.com/dns/logistics/analytics-exporter/internal/metrics"
	"github.com/dns/logistics/analytics-exporter/internal/repository"
	"github.com/dns/logistics/analytics-exporter/internal/state"
	"github.com/dns/logistics/analytics-exporter/internal/worker"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
)

// Run starts analytics-exporter runtime.
func Run(ctx context.Context) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	logger := logging.New(cfg.LogLevel)
	logger.Info("starting service",
		"service", cfg.ServiceName,
		"http_addr", cfg.HTTPAddr,
	)

	shutdownTracing, err := initTracing(ctx, cfg)
	if err != nil {
		return fmt.Errorf("init tracing: %w", err)
	}
	defer shutdownWithTimeout(logger, "tracing", cfg.ShutdownAfter, shutdownTracing)

	meter, shutdownMetrics, err := initMetrics(ctx, cfg)
	if err != nil {
		return fmt.Errorf("init metrics: %w", err)
	}
	defer shutdownWithTimeout(logger, "metrics", cfg.ShutdownAfter, shutdownMetrics)

	metricCollector, err := analyticsmetrics.New(meter)
	if err != nil {
		return fmt.Errorf("init metrics: %w", err)
	}

	dbCtx, dbCancel := context.WithTimeout(ctx, cfg.ConnectTimeout)
	defer dbCancel()

	pgPool, err := repository.NewPool(dbCtx, cfg.PostgresDSN, repository.PoolOptions{
		MaxConns:          cfg.PGMaxConns,
		MinConns:          cfg.PGMinConns,
		MaxConnLifetime:   cfg.PGMaxConnLifetime,
		MaxConnIdleTime:   cfg.PGMaxConnIdleTime,
		HealthCheckPeriod: cfg.PGHealthPeriod,
		ConnectTimeout:    cfg.ConnectTimeout,
	})
	if err != nil {
		return fmt.Errorf("create postgres pool: %w", err)
	}
	defer pgPool.Close()

	tracer := otel.Tracer(cfg.ServiceName)
	repo := repository.New(pgPool, cfg.DBQueryTimeout, tracer, metricCollector)
	if err := repo.Ping(ctx); err != nil {
		return fmt.Errorf("postgres ping failed: %w", err)
	}

	stateStore := state.NewStore(pgPool, cfg.DBQueryTimeout)
	if cfg.EnableAutoMigrate {
		if err := stateStore.EnsureSchema(ctx); err != nil {
			return fmt.Errorf("ensure state schema: %w", err)
		}
	}

	chClient, err := clickhouse.NewClient(cfg, logger)
	if err != nil {
		return fmt.Errorf("init clickhouse client: %w", err)
	}

	exporter := worker.NewExporter(logger, repo, stateStore, chClient, metricCollector, cfg)

	httpHandler := worker.NewHTTPHandler(exporter, metricCollector)
	httpServer := &http.Server{
		Addr:         cfg.HTTPAddr,
		Handler:      otelhttp.NewHandler(httpHandler.Router(), "analytics_exporter_http"),
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		IdleTimeout:  cfg.IdleTimeout,
	}

	errCh := make(chan error, 1)

	go func() {
		if err := exporter.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- fmt.Errorf("exporter failed: %w", err)
		}
	}()

	go func() {
		logger.Info("http server started", "addr", cfg.HTTPAddr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case err := <-errCh:
		return fmt.Errorf("service failed: %w", err)
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.ShutdownAfter)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("http shutdown failed", "error", err)
	}

	return nil
}

func shutdownWithTimeout(
	logger interface{ Error(string, ...any) },
	component string,
	timeout time.Duration,
	fn func(context.Context) error,
) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := fn(ctx); err != nil {
		logger.Error("shutdown failed", "component", component, "error", err)
	}
}

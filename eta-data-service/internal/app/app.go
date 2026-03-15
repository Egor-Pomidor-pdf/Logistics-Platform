package app

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"

	"github.com/dns/logistics/eta-data-service/internal/cache"
	"github.com/dns/logistics/eta-data-service/internal/config"
	"github.com/dns/logistics/eta-data-service/internal/domain"
	"github.com/dns/logistics/eta-data-service/internal/handlers"
	"github.com/dns/logistics/eta-data-service/internal/logging"
	"github.com/dns/logistics/eta-data-service/internal/metrics"
	"github.com/dns/logistics/eta-data-service/internal/repository"
	"github.com/dns/logistics/eta-data-service/internal/service"
	rabbittransport "github.com/dns/logistics/eta-data-service/internal/transport/rabbit"
)

// Run starts eta-data-service runtime.
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

	metricCollector, err := metrics.New(meter)
	if err != nil {
		return fmt.Errorf("init business metrics: %w", err)
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
	pgRepo := repository.NewPostgresRepository(pgPool, cfg.DBQueryTimeout, cfg.DBWriteTimeout, tracer, metricCollector)
	if err := pgRepo.Ping(ctx); err != nil {
		return fmt.Errorf("postgres ping failed: %w", err)
	}

	if cfg.EnableAutoMigrate {
		if err := pgRepo.EnsureSchema(ctx); err != nil {
			return fmt.Errorf("ensure schema: %w", err)
		}
	}

	// Redis cache for shipment features.
	featuresCache := cache.NewRedisFeaturesCache(cfg.RedisAddr, cfg.RedisDB)
	defer featuresCache.Close()

	// RabbitMQ connection and channels.
	rabbitConn, err := amqp.Dial(cfg.RabbitURL)
	if err != nil {
		return fmt.Errorf("dial rabbitmq: %w", err)
	}
	defer rabbitConn.Close()

	pubCh, err := rabbitConn.Channel()
	if err != nil {
		return fmt.Errorf("open rabbit publisher channel: %w", err)
	}
	defer pubCh.Close()

	subCh, err := rabbitConn.Channel()
	if err != nil {
		return fmt.Errorf("open rabbit consumer channel: %w", err)
	}
	defer subCh.Close()

	publisher := rabbittransport.NewPublisher(pubCh, cfg.RabbitExchange, cfg.RabbitRouting)

	// Handler для пересчёта фич в воркере.
	recalcHandler := func(taskCtx context.Context, task rabbittransport.RecalcFeaturesTask) error {
		if task.ShipmentID == "" {
			return fmt.Errorf("empty shipment_id in task")
		}

		now := time.Now().UTC()
		since := now.AddDate(0, 0, -30)

		history, err := pgRepo.ListShipmentHistory(taskCtx, repository.ShipmentHistoryFilter{
			Since: since,
			Until: now,
			Limit: 10000,
		})
		if err != nil {
			return fmt.Errorf("list history: %w", err)
		}

		features := service.BuildFeaturesForShipments(now, history, map[string]domain.ShipmentState{})
		for _, f := range features {
			if f.ShipmentID == task.ShipmentID {
				if err := featuresCache.SetShipmentFeatures(taskCtx, f, cfg.FeatureTTL); err != nil {
					return fmt.Errorf("set features cache: %w", err)
				}
				logger.Info("features recomputed and cached",
					"shipment_id", task.ShipmentID,
					"reason", task.Reason,
				)
				break
			}
		}

		return nil
	}

	consumer, err := rabbittransport.NewConsumer(subCh, cfg.RabbitQueue, cfg.RabbitPrefetch, recalcHandler)
	if err != nil {
		return fmt.Errorf("create rabbit consumer: %w", err)
	}

	// HTTP API.
	httpHandler := handlers.NewHTTPHandler(logger, pgRepo, metricCollector, tracer, featuresCache, publisher)
	httpServer := &http.Server{
		Addr:         cfg.HTTPAddr,
		Handler:      otelhttp.NewHandler(httpHandler.Router(), "eta_data_http"),
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		IdleTimeout:  cfg.IdleTimeout,
	}

	errCh := make(chan error, 1)

	go func() {
		// worker для обработки задач пересчёта фич.
		if err := consumer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- fmt.Errorf("rabbit consumer failed: %w", err)
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
		return fmt.Errorf("http server failed: %w", err)
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

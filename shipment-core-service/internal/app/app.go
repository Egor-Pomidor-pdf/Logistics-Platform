package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	shipmentcorev1 "github.com/dns/logistics/shipment-core-service/internal/api/shipmentcore/v1"
	"github.com/dns/logistics/shipment-core-service/internal/config"
	"github.com/dns/logistics/shipment-core-service/internal/handlers"
	"github.com/dns/logistics/shipment-core-service/internal/logging"
	"github.com/dns/logistics/shipment-core-service/internal/metrics"
	"github.com/dns/logistics/shipment-core-service/internal/middleware"
	"github.com/dns/logistics/shipment-core-service/internal/repository"
	"github.com/dns/logistics/shipment-core-service/internal/service"
	rabbittransport "github.com/dns/logistics/shipment-core-service/internal/transport/rabbit"
)

// Run starts shipment-core-service servers and dependencies.
func Run(ctx context.Context) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	logger := logging.New(cfg.LogLevel)
	logger.Info("starting service",
		"service", cfg.ServiceName,
		"http_addr", cfg.HTTPAddr,
		"grpc_addr", cfg.GRPCAddr,
		"kafka_enabled", cfg.KafkaEnabled,
		"kafka_topic", cfg.KafkaTopic,
		"kafka_group_id", cfg.KafkaGroupID,
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
	pool, err := repository.NewPool(dbCtx, cfg.PostgresDSN, repository.PoolOptions{
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
	defer pool.Close()

	repo := repository.NewPostgresRepository(
		pool,
		cfg.DBQueryTimeout,
		cfg.DBWriteTimeout,
		otel.Tracer(cfg.ServiceName),
		metricCollector,
	)

	if err := repo.Ping(ctx); err != nil {
		return fmt.Errorf("postgres ping failed: %w", err)
	}
	if cfg.EnableAutoMigrate {
		if err := repo.EnsureSchema(ctx); err != nil {
			return fmt.Errorf("ensure schema: %w", err)
		}
	}

	coreSvc := service.New(repo, metricCollector, otel.Tracer(cfg.ServiceName))

	// RabbitMQ publisher for ETA-data feature recomputation tasks.
	rabbitConn, err := amqp.Dial(cfg.RabbitURL)
	if err != nil {
		return fmt.Errorf("dial rabbitmq: %w", err)
	}
	defer rabbitConn.Close()

	rabbitCh, err := rabbitConn.Channel()
	if err != nil {
		return fmt.Errorf("open rabbit channel: %w", err)
	}
	defer rabbitCh.Close()

	etaPublisher := rabbittransport.NewPublisher(rabbitCh, cfg.RabbitExchange, cfg.RabbitRouting)

	coreSvc.WithFeatureTaskPublisher(func(ctx context.Context, shipmentID string, reason string, force bool) error {
		task := rabbittransport.RecalcFeaturesTask{
			ShipmentID: shipmentID,
			Reason:     reason,
			Force:      force,
		}
		return etaPublisher.PublishRecalcFeatures(ctx, task)
	})

	jwtSecret := []byte(cfg.JWTSecret)

	httpHandler := handlers.NewHTTPHandler(logger, coreSvc, metricCollector, jwtSecret)
	grpcHandler := handlers.NewGRPCHandler(logger, coreSvc, metricCollector)

	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	grpcInterceptors := []grpc.UnaryServerInterceptor{
		grpcLoggingInterceptor(logger),
	}
	if len(jwtSecret) > 0 {
		grpcInterceptors = append(grpcInterceptors,
			middleware.GRPCAuthInterceptor(jwtSecret, logger),
			middleware.GRPCRequireRoles(
				middleware.RoleWarehouse,
				middleware.RoleStore,
				middleware.RoleBackoffice,
				middleware.RoleAdmin,
			),
		)
	}

	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(grpcInterceptors...),
	)
	shipmentcorev1.RegisterShipmentCoreServiceServer(grpcServer, grpcHandler)
	healthpb.RegisterHealthServer(grpcServer, healthServer)

	grpcListener, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen grpc: %w", err)
	}
	defer grpcListener.Close()

	httpServer := &http.Server{
		Addr:         cfg.HTTPAddr,
		Handler:      otelhttp.NewHandler(httpHandler.Router(), "shipment_core_http"),
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		IdleTimeout:  cfg.IdleTimeout,
	}

	httpErrCh := make(chan error, 1)
	grpcErrCh := make(chan error, 1)
	consumerErrCh := make(chan error, 1)

	if cfg.KafkaEnabled {
		consumer := handlers.NewKafkaConsumer(
			logger,
			coreSvc,
			metricCollector,
			otel.Tracer(cfg.ServiceName),
			cfg.KafkaBrokers,
			cfg.KafkaTopic,
			cfg.KafkaGroupID,
		)
		go func() {
			logger.Info("kafka consumer started", "topic", cfg.KafkaTopic, "group_id", cfg.KafkaGroupID)
			if err := consumer.Run(ctx); err != nil {
				consumerErrCh <- err
			}
		}()
	} else {
		logger.Warn("kafka consumer disabled by config")
	}

	go func() {
		logger.Info("http server started", "addr", cfg.HTTPAddr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			httpErrCh <- err
		}
	}()

	go func() {
		logger.Info("grpc server started", "addr", cfg.GRPCAddr)
		if err := grpcServer.Serve(grpcListener); err != nil {
			grpcErrCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case err := <-consumerErrCh:
		return fmt.Errorf("kafka consumer failed: %w", err)
	case err := <-httpErrCh:
		return fmt.Errorf("http server failed: %w", err)
	case err := <-grpcErrCh:
		return fmt.Errorf("grpc server failed: %w", err)
	}

	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.ShutdownAfter)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("http shutdown failed", "error", err)
	}

	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
	case <-shutdownCtx.Done():
		grpcServer.Stop()
	}

	return nil
}

func grpcLoggingInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		st := status.Convert(err)

		logFields := []any{
			"grpc_method", info.FullMethod,
			"grpc_code", st.Code().String(),
			"duration_ms", time.Since(start).Milliseconds(),
		}
		logFields = append(logFields, logging.TraceFields(ctx)...)

		if err != nil {
			logger.Warn("grpc request completed with error", append(logFields, "error", err)...)
			if span := trace.SpanFromContext(ctx); span.SpanContext().IsValid() {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
			}
			return nil, err
		}

		logger.Info("grpc request completed", logFields...)
		return resp, nil
	}
}

func shutdownWithTimeout(logger *slog.Logger, component string, timeout time.Duration, fn func(context.Context) error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := fn(ctx); err != nil {
		logger.Error("shutdown failed", "component", component, "error", err)
	}
}

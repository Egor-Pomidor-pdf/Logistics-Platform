package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"

	"github.com/dns/logistics/logistics-normalizer-service/internal/config"
	"github.com/dns/logistics/logistics-normalizer-service/internal/handlers"
	"github.com/dns/logistics/logistics-normalizer-service/internal/logging"
	"github.com/dns/logistics/logistics-normalizer-service/internal/metrics"
	"github.com/dns/logistics/logistics-normalizer-service/internal/repository"
)

// Run starts logistics-normalizer-service.
func Run(ctx context.Context) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	logger := logging.New(cfg.LogLevel)
	logger.Info(
		"starting service",
		"service", cfg.ServiceName,
		"raw_topic", cfg.RawTopic,
		"normalized_topic", cfg.NormalizedTopic,
		"invalid_topic", cfg.InvalidTopic,
	)

	shutdownTracing, err := initTracing(ctx, cfg)
	if err != nil {
		return fmt.Errorf("init tracing: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if traceErr := shutdownTracing(shutdownCtx); traceErr != nil {
			logger.Error("tracing shutdown failed", "error", traceErr)
		}
	}()

	normalizedEventsCounter, shutdownMetricsSDK, err := initMetrics(ctx, cfg)
	if err != nil {
		return fmt.Errorf("init metrics: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if metricErr := shutdownMetricsSDK(shutdownCtx); metricErr != nil {
			logger.Error("metrics sdk shutdown failed", "error", metricErr)
		}
	}()

	collector := metrics.New()
	metricsServer := startMetricsServer(logger, cfg.MetricsAddr, collector)
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := metricsServer.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("metrics server shutdown failed", "error", err)
		}
	}()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  cfg.KafkaBrokers,
		Topic:    cfg.RawTopic,
		GroupID:  cfg.GroupID,
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer func() {
		if err := reader.Close(); err != nil {
			logger.Error("failed to close Kafka reader", "error", err)
		}
	}()

	normalizedWriter := &kafka.Writer{
		Addr:     kafka.TCP(cfg.KafkaBrokers...),
		Topic:    cfg.NormalizedTopic,
		Balancer: &kafka.Hash{},
	}
	defer func() {
		if err := normalizedWriter.Close(); err != nil {
			logger.Error("failed to close normalized writer", "error", err)
		}
	}()

	invalidWriter := &kafka.Writer{
		Addr:     kafka.TCP(cfg.KafkaBrokers...),
		Topic:    cfg.InvalidTopic,
		Balancer: &kafka.Hash{},
	}
	defer func() {
		if err := invalidWriter.Close(); err != nil {
			logger.Error("failed to close invalid writer", "error", err)
		}
	}()

	var deduplicator repository.EventDeduplicator
	if cfg.RedisAddr != "" {
		deduplicator = repository.NewRedisDeduplicator(cfg.RedisAddr, cfg.RedisDedupTTL)
		defer func() {
			if err := deduplicator.Close(); err != nil {
				logger.Error("failed to close redis deduplicator", "error", err)
			}
		}()
	}

	kafkaHandler := handlers.NewKafkaHandler(
		logger,
		collector,
		deduplicator,
		normalizedWriter,
		invalidWriter,
		otel.Tracer(cfg.ServiceName),
		normalizedEventsCounter,
	)

	httpHandler := handlers.NewHTTPHandler(
		logger,
		collector,
		deduplicator,
		normalizedWriter,
		invalidWriter,
		otel.Tracer(cfg.ServiceName),
		normalizedEventsCounter,
	)

	apiServer := &http.Server{
		Addr:         cfg.HTTPAddr,
		Handler:      httpHandler.Router(),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := apiServer.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("api server shutdown failed", "error", err)
		}
	}()

	go func() {
		logger.Info("REST API server started", "addr", cfg.HTTPAddr)
		if err := apiServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("REST API server stopped with error", "error", err)
		}
	}()

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				logger.Info("shutdown signal received")
				return nil
			}
			logger.Error("failed to fetch Kafka message", "error", err)
			continue
		}

		if err := kafkaHandler.ProcessMessage(ctx, msg); err != nil {
			logger.Error(
				"failed to process raw message",
				"error", err,
				"topic", msg.Topic,
				"partition", msg.Partition,
				"offset", msg.Offset,
			)
			continue
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			logger.Error("failed to commit Kafka message", "error", err, "offset", msg.Offset)
		}
	}
}

func startMetricsServer(logger *slog.Logger, addr string, collector *metrics.Collector) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", collector.Handler())

	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		logger.Info("metrics server started", "addr", addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("metrics server stopped with error", "error", err)
		}
	}()

	return server
}

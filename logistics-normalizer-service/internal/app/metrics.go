package app

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	"github.com/dns/logistics/logistics-normalizer-service/internal/config"
)

func initMetrics(ctx context.Context, cfg config.Config) (metric.Int64Counter, func(context.Context) error, error) {
	if cfg.OTLPEndpoint == "" {
		meter := otel.Meter(telemetryServiceName)
		counter, err := meter.Int64Counter("logistics_normalized_events_total")
		if err != nil {
			return nil, nil, fmt.Errorf("create noop normalized events counter: %w", err)
		}
		return counter, func(context.Context) error { return nil }, nil
	}

	exporter, err := otlpmetricgrpc.New(
		ctx,
		otlpmetricgrpc.WithEndpoint(cfg.OTLPEndpoint),
		otlpmetricgrpc.WithInsecure(),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("create otlp metric exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceNameKey.String(telemetryServiceName)),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("create metric resource: %w", err)
	}

	reader := sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(10*time.Second))
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(provider)

	meter := provider.Meter(telemetryServiceName)
	counter, err := meter.Int64Counter(
		"logistics_normalized_events_total",
		metric.WithDescription("Total number of normalized logistics events"),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("create normalized events counter: %w", err)
	}

	return counter, provider.Shutdown, nil
}

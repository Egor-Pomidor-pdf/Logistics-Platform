package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/kelseyhightower/envconfig"
)

const (
	defaultRawTopic        = "raw_logistics_events"
	defaultNormalizedTopic = "normalized_logistics_events"
	defaultInvalidTopic    = "logistics_events_invalid"
	defaultGroupID         = "logistics-normalizer"
	defaultServiceName     = "logistics-normalizer"
	defaultLogLevel        = "info"
	defaultHTTPAddr        = ":8081"
	defaultMetricsAddr     = ":2112"
	defaultRedisDedupTTL   = "24h"
)

// Config keeps service runtime settings.
type Config struct {
	KafkaBrokers    []string
	RawTopic        string
	NormalizedTopic string
	InvalidTopic    string
	GroupID         string
	RedisAddr       string
	ServiceName     string
	LogLevel        string
	HTTPAddr        string
	OTLPEndpoint    string
	MetricsAddr     string
	RedisDedupTTL   time.Duration
}

type envCfg struct {
	KafkaBrokers    string `envconfig:"KAFKA_BROKERS" default:"kafka:9092"`
	RawTopic        string `envconfig:"RAW_TOPIC" default:"raw_logistics_events"`
	NormalizedTopic string `envconfig:"NORMALIZED_TOPIC" default:"normalized_logistics_events"`
	InvalidTopic    string `envconfig:"INVALID_TOPIC" default:"logistics_events_invalid"`
	GroupID         string `envconfig:"GROUP_ID" default:"logistics-normalizer"`
	RedisAddr       string `envconfig:"REDIS_ADDR" default:"redis:6379"`
	ServiceName     string `envconfig:"SERVICE_NAME" default:"logistics-normalizer"`
	LogLevel        string `envconfig:"LOG_LEVEL" default:"info"`
	OTLPEndpoint    string `envconfig:"OTLP_ENDPOINT"`
	LegacyOTLP      string `envconfig:"OTEL_EXPORTER_OTLP_ENDPOINT"`
	HTTPAddr        string `envconfig:"HTTP_ADDR" default:":8081"`
	MetricsAddr     string `envconfig:"METRICS_ADDR" default":2112"`
	RedisDedupTTL   string `envconfig:"REDIS_DEDUP_TTL" default:"24h"`
}

// Load reads config from environment.
func Load() (Config, error) {
	raw := envCfg{}
	if err := envconfig.Process("", &raw); err != nil {
		return Config{}, fmt.Errorf("load env config: %w", err)
	}

	brokers := splitCSV(raw.KafkaBrokers)
	if len(brokers) == 0 {
		return Config{}, fmt.Errorf("KAFKA_BROKERS is empty")
	}

	ttl, err := time.ParseDuration(raw.RedisDedupTTL)
	if err != nil {
		return Config{}, fmt.Errorf("parse REDIS_DEDUP_TTL: %w", err)
	}

	cfg := Config{
		KafkaBrokers:    brokers,
		RawTopic:        firstNonEmpty(raw.RawTopic, defaultRawTopic),
		NormalizedTopic: firstNonEmpty(raw.NormalizedTopic, defaultNormalizedTopic),
		InvalidTopic:    firstNonEmpty(raw.InvalidTopic, defaultInvalidTopic),
		GroupID:         firstNonEmpty(raw.GroupID, defaultGroupID),
		RedisAddr:       strings.TrimSpace(raw.RedisAddr),
		ServiceName:     firstNonEmpty(raw.ServiceName, defaultServiceName),
		LogLevel:        firstNonEmpty(raw.LogLevel, defaultLogLevel),
		HTTPAddr:        firstNonEmpty(raw.HTTPAddr, defaultHTTPAddr),
		OTLPEndpoint:    firstNonEmpty(strings.TrimSpace(raw.OTLPEndpoint), strings.TrimSpace(raw.LegacyOTLP)),
		MetricsAddr:     firstNonEmpty(raw.MetricsAddr, defaultMetricsAddr),
		RedisDedupTTL:   ttl,
	}

	return cfg, nil
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		s := strings.TrimSpace(p)
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}

func firstNonEmpty(value, fallback string) string {
	v := strings.TrimSpace(value)
	if v == "" {
		return fallback
	}
	return v
}

package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/kelseyhightower/envconfig"
)

const (
	defaultServiceName        = "eta-data-service"
	defaultLogLevel           = "info"
	defaultHTTPAddr           = ":8083"
	defaultShutdownTimeout    = "15s"
	defaultReadTimeout        = "5s"
	defaultWriteTimeout       = "10s"
	defaultIdleTimeout        = "60s"
	defaultConnectTimeout     = "3s"
	defaultQueryTimeout       = "3s"
	defaultWriteQueryTimeout  = "5s"
	defaultPGMaxConns         = 40
	defaultPGMinConns         = 5
	defaultPGMaxConnLifetime  = "30m"
	defaultPGMaxConnIdleTime  = "10m"
	defaultPGHealthCheck      = "30s"
	defaultOTLPMetricInterval = "10s"
)

// Config holds all runtime settings for eta-data-service.
type Config struct {
	ServiceName string
	LogLevel    string

	HTTPAddr      string
	ReadTimeout   time.Duration
	WriteTimeout  time.Duration
	IdleTimeout   time.Duration
	ShutdownAfter time.Duration

	PostgresDSN       string
	DBQueryTimeout    time.Duration
	DBWriteTimeout    time.Duration
	ConnectTimeout    time.Duration
	PGMaxConns        int32
	PGMinConns        int32
	PGMaxConnLifetime time.Duration
	PGMaxConnIdleTime time.Duration
	PGHealthPeriod    time.Duration

	EnableAutoMigrate bool

	// Telemetry
	OTLPEndpoint       string
	OTLPMetricInterval time.Duration

	// Redis cache for shipment features
	RedisAddr  string
	RedisDB    int
	FeatureTTL time.Duration

	// RabbitMQ for async feature recomputation
	RabbitURL      string
	RabbitExchange string
	RabbitQueue    string
	RabbitRouting  string
	RabbitDLQ      string
	RabbitPrefetch int
	WorkerCount    int
}

type envCfg struct {
	ServiceName string `envconfig:"SERVICE_NAME" default:"eta-data-service"`
	LogLevel    string `envconfig:"LOG_LEVEL" default:"info"`

	HTTPAddr     string `envconfig:"HTTP_ADDR" default:":8083"`
	ReadTimeout  string `envconfig:"HTTP_READ_TIMEOUT" default:"5s"`
	WriteTimeout string `envconfig:"HTTP_WRITE_TIMEOUT" default:"10s"`
	IdleTimeout  string `envconfig:"HTTP_IDLE_TIMEOUT" default:"60s"`
	Shutdown     string `envconfig:"SHUTDOWN_TIMEOUT" default:"15s"`

	PostgresDSN       string `envconfig:"POSTGRES_DSN" required:"true"`
	DBQueryTimeout    string `envconfig:"DB_QUERY_TIMEOUT" default:"3s"`
	DBWriteTimeout    string `envconfig:"DB_WRITE_TIMEOUT" default:"5s"`
	ConnectTimeout    string `envconfig:"DB_CONNECT_TIMEOUT" default:"3s"`
	PGMaxConns        int32  `envconfig:"PG_MAX_CONNS" default:"40"`
	PGMinConns        int32  `envconfig:"PG_MIN_CONNS" default:"5"`
	PGMaxConnLifetime string `envconfig:"PG_MAX_CONN_LIFETIME" default:"30m"`
	PGMaxConnIdleTime string `envconfig:"PG_MAX_CONN_IDLE_TIME" default:"10m"`
	PGHealthPeriod    string `envconfig:"PG_HEALTH_CHECK_PERIOD" default:"30s"`
	EnableAutoMigrate bool   `envconfig:"ENABLE_AUTO_MIGRATE" default:"false"`

	OTLPEndpoint string `envconfig:"OTLP_ENDPOINT"`
	LegacyOTLP   string `envconfig:"OTEL_EXPORTER_OTLP_ENDPOINT"`
	OTLPInterval string `envconfig:"OTLP_METRIC_INTERVAL" default:"10s"`

	// Redis
	RedisAddr  string `envconfig:"REDIS_ADDR" default:"redis:6379"`
	RedisDB    int    `envconfig:"REDIS_DB" default:"0"`
	FeatureTTL string `envconfig:"FEATURE_TTL" default:"10m"`

	// RabbitMQ
	RabbitURL      string `envconfig:"RABBITMQ_URL" default:"amqp://guest:guest@rabbitmq:5672/"`
	RabbitExchange string `envconfig:"RABBITMQ_EXCHANGE" default:"eta_data_exchange"`
	RabbitQueue    string `envconfig:"RABBITMQ_QUEUE" default:"eta_data_recalc"`
	RabbitRouting  string `envconfig:"RABBITMQ_ROUTING_KEY" default:"eta_data.recalc"`
	RabbitDLQ      string `envconfig:"RABBITMQ_DLQ" default:"eta_data_recalc.dlq"`
	RabbitPrefetch int    `envconfig:"RABBITMQ_PREFETCH" default:"64"`
	WorkerCount    int    `envconfig:"WORKER_COUNT" default:"8"`
}

// Load reads environment and validates critical values.
func Load() (Config, error) {
	raw := envCfg{}
	if err := envconfig.Process("", &raw); err != nil {
		return Config{}, fmt.Errorf("load env config: %w", err)
	}

	cfg := Config{
		ServiceName:       firstNonEmpty(raw.ServiceName, defaultServiceName),
		LogLevel:          firstNonEmpty(raw.LogLevel, defaultLogLevel),
		HTTPAddr:          firstNonEmpty(raw.HTTPAddr, defaultHTTPAddr),
		PostgresDSN:       strings.TrimSpace(raw.PostgresDSN),
		PGMaxConns:        raw.PGMaxConns,
		PGMinConns:        raw.PGMinConns,
		EnableAutoMigrate: raw.EnableAutoMigrate,
		OTLPEndpoint:      firstNonEmpty(strings.TrimSpace(raw.OTLPEndpoint), strings.TrimSpace(raw.LegacyOTLP)),

		RedisAddr: raw.RedisAddr,
		RedisDB:   raw.RedisDB,

		RabbitURL:      raw.RabbitURL,
		RabbitExchange: raw.RabbitExchange,
		RabbitQueue:    raw.RabbitQueue,
		RabbitRouting:  raw.RabbitRouting,
		RabbitDLQ:      raw.RabbitDLQ,
		RabbitPrefetch: raw.RabbitPrefetch,
		WorkerCount:    raw.WorkerCount,
	}

	var err error

	if cfg.ReadTimeout, err = parseDuration(raw.ReadTimeout, defaultReadTimeout); err != nil {
		return Config{}, fmt.Errorf("parse HTTP_READ_TIMEOUT: %w", err)
	}
	if cfg.WriteTimeout, err = parseDuration(raw.WriteTimeout, defaultWriteTimeout); err != nil {
		return Config{}, fmt.Errorf("parse HTTP_WRITE_TIMEOUT: %w", err)
	}
	if cfg.IdleTimeout, err = parseDuration(raw.IdleTimeout, defaultIdleTimeout); err != nil {
		return Config{}, fmt.Errorf("parse HTTP_IDLE_TIMEOUT: %w", err)
	}
	if cfg.ShutdownAfter, err = parseDuration(raw.Shutdown, defaultShutdownTimeout); err != nil {
		return Config{}, fmt.Errorf("parse SHUTDOWN_TIMEOUT: %w", err)
	}
	if cfg.DBQueryTimeout, err = parseDuration(raw.DBQueryTimeout, defaultQueryTimeout); err != nil {
		return Config{}, fmt.Errorf("parse DB_QUERY_TIMEOUT: %w", err)
	}
	if cfg.DBWriteTimeout, err = parseDuration(raw.DBWriteTimeout, defaultWriteQueryTimeout); err != nil {
		return Config{}, fmt.Errorf("parse DB_WRITE_TIMEOUT: %w", err)
	}
	if cfg.ConnectTimeout, err = parseDuration(raw.ConnectTimeout, defaultConnectTimeout); err != nil {
		return Config{}, fmt.Errorf("parse DB_CONNECT_TIMEOUT: %w", err)
	}
	if cfg.PGMaxConnLifetime, err = parseDuration(raw.PGMaxConnLifetime, defaultPGMaxConnLifetime); err != nil {
		return Config{}, fmt.Errorf("parse PG_MAX_CONN_LIFETIME: %w", err)
	}
	if cfg.PGMaxConnIdleTime, err = parseDuration(raw.PGMaxConnIdleTime, defaultPGMaxConnIdleTime); err != nil {
		return Config{}, fmt.Errorf("parse PG_MAX_CONN_IDLE_TIME: %w", err)
	}
	if cfg.PGHealthPeriod, err = parseDuration(raw.PGHealthPeriod, defaultPGHealthCheck); err != nil {
		return Config{}, fmt.Errorf("parse PG_HEALTH_CHECK_PERIOD: %w", err)
	}
	if cfg.OTLPMetricInterval, err = parseDuration(raw.OTLPInterval, defaultOTLPMetricInterval); err != nil {
		return Config{}, fmt.Errorf("parse OTLP_METRIC_INTERVAL: %w", err)
	}
	if cfg.FeatureTTL, err = parseDuration(raw.FeatureTTL, "10m"); err != nil {
		return Config{}, fmt.Errorf("parse FEATURE_TTL: %w", err)
	}

	if cfg.PostgresDSN == "" {
		return Config{}, fmt.Errorf("POSTGRES_DSN is required")
	}
	if cfg.PGMaxConns <= 0 {
		cfg.PGMaxConns = defaultPGMaxConns
	}
	if cfg.PGMinConns < 0 {
		cfg.PGMinConns = defaultPGMinConns
	}
	if cfg.PGMinConns > cfg.PGMaxConns {
		cfg.PGMinConns = cfg.PGMaxConns
	}

	if cfg.WorkerCount <= 0 {
		cfg.WorkerCount = 8
	}
	if cfg.RabbitPrefetch <= 0 {
		cfg.RabbitPrefetch = 64
	}

	return cfg, nil
}

func parseDuration(value, fallback string) (time.Duration, error) {
	if strings.TrimSpace(value) == "" {
		value = fallback
	}
	return time.ParseDuration(value)
}

func firstNonEmpty(value, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	return value
}


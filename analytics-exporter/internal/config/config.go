package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/kelseyhightower/envconfig"
)

const (
	defaultServiceName     = "analytics-exporter"
	defaultLogLevel        = "info"
	defaultHTTPAddr        = ":8090"
	defaultShutdownTimeout = "15s"
	defaultReadTimeout     = "5s"
	defaultWriteTimeout    = "10s"
	defaultIdleTimeout     = "60s"

	defaultConnectTimeout    = "3s"
	defaultQueryTimeout      = "5s"
	defaultPGMaxConns        = 20
	defaultPGMinConns        = 2
	defaultPGMaxConnLifetime = "30m"
	defaultPGMaxConnIdleTime = "10m"
	defaultPGHealthCheck     = "30s"

	defaultOTLPMetricInterval = "10s"

	defaultExportInterval    = "1m"
	defaultExportLag         = "1m"
	defaultBatchSize         = 5000
	defaultBackfillWindow    = "720h"
	defaultClickHouseURL     = "http://clickhouse:8123"
	defaultClickHouseDB      = "default"
	defaultClickHouseTimeout = "5s"
)

// Config holds analytics-exporter runtime settings.
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

	// Export scheduling
	ExportInterval time.Duration
	ExportLag      time.Duration
	ExportBatch    int
	BackfillWindow time.Duration

	ExportHistory bool
	ExportState   bool

	// ClickHouse HTTP
	ClickHouseURL      string
	ClickHouseDatabase string
	ClickHouseUser     string
	ClickHousePassword string
	ClickHouseTimeout  time.Duration
	ClickHouseHistory  string
	ClickHouseState    string
}

type envCfg struct {
	ServiceName string `envconfig:"SERVICE_NAME" default:"analytics-exporter"`
	LogLevel    string `envconfig:"LOG_LEVEL" default:"info"`

	HTTPAddr     string `envconfig:"HTTP_ADDR" default":8090"`
	ReadTimeout  string `envconfig:"HTTP_READ_TIMEOUT" default:"5s"`
	WriteTimeout string `envconfig:"HTTP_WRITE_TIMEOUT" default:"10s"`
	IdleTimeout  string `envconfig:"HTTP_IDLE_TIMEOUT" default:"60s"`
	Shutdown     string `envconfig:"SHUTDOWN_TIMEOUT" default:"15s"`

	PostgresDSN       string `envconfig:"POSTGRES_DSN" required:"true"`
	DBQueryTimeout    string `envconfig:"DB_QUERY_TIMEOUT" default:"5s"`
	ConnectTimeout    string `envconfig:"DB_CONNECT_TIMEOUT" default:"3s"`
	PGMaxConns        int32  `envconfig:"PG_MAX_CONNS" default:"20"`
	PGMinConns        int32  `envconfig:"PG_MIN_CONNS" default:"2"`
	PGMaxConnLifetime string `envconfig:"PG_MAX_CONN_LIFETIME" default:"30m"`
	PGMaxConnIdleTime string `envconfig:"PG_MAX_CONN_IDLE_TIME" default:"10m"`
	PGHealthPeriod    string `envconfig:"PG_HEALTH_CHECK_PERIOD" default:"30s"`
	EnableAutoMigrate bool   `envconfig:"ENABLE_AUTO_MIGRATE" default:"true"`

	OTLPEndpoint string `envconfig:"OTLP_ENDPOINT"`
	LegacyOTLP   string `envconfig:"OTEL_EXPORTER_OTLP_ENDPOINT"`
	OTLPInterval string `envconfig:"OTLP_METRIC_INTERVAL" default:"10s"`

	ExportInterval string `envconfig:"EXPORT_INTERVAL" default:"1m"`
	ExportLag      string `envconfig:"EXPORT_LAG" default:"1m"`
	ExportBatch    int    `envconfig:"EXPORT_BATCH_SIZE" default:"5000"`
	BackfillWindow string `envconfig:"EXPORT_BACKFILL_WINDOW" default:"720h"`

	ExportHistory bool `envconfig:"EXPORT_HISTORY" default:"true"`
	ExportState   bool `envconfig:"EXPORT_STATE" default:"true"`

	ClickHouseURL      string `envconfig:"CLICKHOUSE_URL" default:"http://clickhouse:8123"`
	ClickHouseDatabase string `envconfig:"CLICKHOUSE_DATABASE" default:"default"`
	ClickHouseUser     string `envconfig:"CLICKHOUSE_USER"`
	ClickHousePassword string `envconfig:"CLICKHOUSE_PASSWORD"`
	ClickHouseTimeout  string `envconfig:"CLICKHOUSE_TIMEOUT" default:"5s"`
	ClickHouseHistory  string `envconfig:"CLICKHOUSE_HISTORY_TABLE" default:"shipment_history"`
	ClickHouseState    string `envconfig:"CLICKHOUSE_STATE_TABLE" default:"shipment_state"`
}

// Load reads environment and validates values.
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

		ExportBatch:   raw.ExportBatch,
		ExportHistory: raw.ExportHistory,
		ExportState:   raw.ExportState,

		ClickHouseURL:      firstNonEmpty(strings.TrimSpace(raw.ClickHouseURL), defaultClickHouseURL),
		ClickHouseDatabase: firstNonEmpty(strings.TrimSpace(raw.ClickHouseDatabase), defaultClickHouseDB),
		ClickHouseUser:     strings.TrimSpace(raw.ClickHouseUser),
		ClickHousePassword: strings.TrimSpace(raw.ClickHousePassword),
		ClickHouseHistory:  strings.TrimSpace(raw.ClickHouseHistory),
		ClickHouseState:    strings.TrimSpace(raw.ClickHouseState),
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
	if cfg.ExportInterval, err = parseDuration(raw.ExportInterval, defaultExportInterval); err != nil {
		return Config{}, fmt.Errorf("parse EXPORT_INTERVAL: %w", err)
	}
	if cfg.ExportLag, err = parseDuration(raw.ExportLag, defaultExportLag); err != nil {
		return Config{}, fmt.Errorf("parse EXPORT_LAG: %w", err)
	}
	if cfg.BackfillWindow, err = parseDuration(raw.BackfillWindow, defaultBackfillWindow); err != nil {
		return Config{}, fmt.Errorf("parse EXPORT_BACKFILL_WINDOW: %w", err)
	}
	if cfg.ClickHouseTimeout, err = parseDuration(raw.ClickHouseTimeout, defaultClickHouseTimeout); err != nil {
		return Config{}, fmt.Errorf("parse CLICKHOUSE_TIMEOUT: %w", err)
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
	if cfg.ExportBatch <= 0 {
		cfg.ExportBatch = defaultBatchSize
	}
	if cfg.ClickHouseHistory == "" {
		cfg.ClickHouseHistory = "shipment_history"
	}
	if cfg.ClickHouseState == "" {
		cfg.ClickHouseState = "shipment_state"
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

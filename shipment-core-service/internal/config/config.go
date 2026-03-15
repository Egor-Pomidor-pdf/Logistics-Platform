package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/kelseyhightower/envconfig"
)

const (
	defaultServiceName        = "shipment-core-service"
	defaultLogLevel           = "info"
	defaultHTTPAddr           = ":8080"
	defaultGRPCAddr           = ":9090"
	defaultKafkaTopic         = "normalized_logistics_events"
	defaultKafkaGroupID       = "shipment-core-service"
	defaultShutdownTimeout    = "10s"
	defaultReadTimeout        = "5s"
	defaultWriteTimeout       = "10s"
	defaultIdleTimeout        = "60s"
	defaultQueryTimeout       = "3s"
	defaultWriteQueryTimeout  = "5s"
	defaultMaxConns           = 60
	defaultMinConns           = 10
	defaultMaxConnLifetime    = "30m"
	defaultMaxConnIdleTime    = "10m"
	defaultHealthCheckPeriod  = "30s"
	defaultConnectTimeout     = "3s"
	defaultOTLPMetricInterval = "10s"
)

// Config stores all runtime settings.
type Config struct {
	ServiceName string
	LogLevel    string

	HTTPAddr      string
	GRPCAddr      string
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

	OTLPEndpoint       string
	OTLPMetricInterval time.Duration
	KafkaBrokers       []string
	KafkaTopic         string
	KafkaGroupID       string
	KafkaEnabled       bool

	// RabbitMQ publisher for ETA-data feature recomputation tasks.
	RabbitURL      string
	RabbitExchange string
	RabbitRouting  string

	// JWT secret for role-based authorization.
	JWTSecret string
}

type envCfg struct {
	ServiceName string `envconfig:"SERVICE_NAME" default:"shipment-core-service"`
	LogLevel    string `envconfig:"LOG_LEVEL" default:"info"`

	HTTPAddr     string `envconfig:"HTTP_ADDR" default":8080"`
	GRPCAddr     string `envconfig:"GRPC_ADDR" default":9090"`
	ReadTimeout  string `envconfig:"HTTP_READ_TIMEOUT" default:"5s"`
	WriteTimeout string `envconfig:"HTTP_WRITE_TIMEOUT" default:"10s"`
	IdleTimeout  string `envconfig:"HTTP_IDLE_TIMEOUT" default:"60s"`
	Shutdown     string `envconfig:"SHUTDOWN_TIMEOUT" default:"10s"`

	PostgresDSN       string `envconfig:"POSTGRES_DSN" required:"true"`
	DBQueryTimeout    string `envconfig:"DB_QUERY_TIMEOUT" default:"3s"`
	DBWriteTimeout    string `envconfig:"DB_WRITE_TIMEOUT" default:"5s"`
	ConnectTimeout    string `envconfig:"DB_CONNECT_TIMEOUT" default:"3s"`
	PGMaxConns        int32  `envconfig:"PG_MAX_CONNS" default:"60"`
	PGMinConns        int32  `envconfig:"PG_MIN_CONNS" default:"10"`
	PGMaxConnLifetime string `envconfig:"PG_MAX_CONN_LIFETIME" default:"30m"`
	PGMaxConnIdleTime string `envconfig:"PG_MAX_CONN_IDLE_TIME" default:"10m"`
	PGHealthPeriod    string `envconfig:"PG_HEALTH_CHECK_PERIOD" default:"30s"`
	EnableAutoMigrate bool   `envconfig:"ENABLE_AUTO_MIGRATE" default:"true"`

	OTLPEndpoint string `envconfig:"OTLP_ENDPOINT"`
	LegacyOTLP   string `envconfig:"OTEL_EXPORTER_OTLP_ENDPOINT"`
	OTLPInterval string `envconfig:"OTLP_METRIC_INTERVAL" default:"10s"`
	KafkaBrokers string `envconfig:"KAFKA_BROKERS" default:"kafka:9092"`
	KafkaTopic   string `envconfig:"NORMALIZED_TOPIC" default:"normalized_logistics_events"`
	KafkaGroupID string `envconfig:"KAFKA_GROUP_ID" default:"shipment-core-service"`
	KafkaEnabled bool   `envconfig:"KAFKA_CONSUMER_ENABLED" default:"true"`

	RabbitURL      string `envconfig:"RABBITMQ_URL" default:"amqp://guest:guest@rabbitmq:5672/"`
	RabbitExchange string `envconfig:"RABBITMQ_EXCHANGE" default:"eta_data_exchange"`
	RabbitRouting  string `envconfig:"RABBITMQ_ROUTING_KEY" default:"eta_data.recalc"`

	JWTSecret string `envconfig:"JWT_SECRET"`

}

// Load reads env and validates values.
func Load() (Config, error) {
	raw := envCfg{}
	if err := envconfig.Process("", &raw); err != nil {
		return Config{}, fmt.Errorf("load env config: %w", err)
	}

	cfg := Config{
		ServiceName:       firstNonEmpty(raw.ServiceName, defaultServiceName),
		LogLevel:          firstNonEmpty(raw.LogLevel, defaultLogLevel),
		HTTPAddr:          firstNonEmpty(raw.HTTPAddr, defaultHTTPAddr),
		GRPCAddr:          firstNonEmpty(raw.GRPCAddr, defaultGRPCAddr),
		PostgresDSN:       strings.TrimSpace(raw.PostgresDSN),
		PGMaxConns:        raw.PGMaxConns,
		PGMinConns:        raw.PGMinConns,
		EnableAutoMigrate: raw.EnableAutoMigrate,
		OTLPEndpoint: firstNonEmpty(strings.TrimSpace(raw.OTLPEndpoint), strings.TrimSpace(raw.LegacyOTLP)),
		KafkaBrokers: splitCSV(raw.KafkaBrokers),
		KafkaTopic:   firstNonEmpty(raw.KafkaTopic, defaultKafkaTopic),
		KafkaGroupID: firstNonEmpty(raw.KafkaGroupID, defaultKafkaGroupID),
		KafkaEnabled: raw.KafkaEnabled,

		RabbitURL:      raw.RabbitURL,
		RabbitExchange: raw.RabbitExchange,
		RabbitRouting:  raw.RabbitRouting,

		JWTSecret: raw.JWTSecret,
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
	if cfg.PGMaxConnLifetime, err = parseDuration(raw.PGMaxConnLifetime, defaultMaxConnLifetime); err != nil {
		return Config{}, fmt.Errorf("parse PG_MAX_CONN_LIFETIME: %w", err)
	}
	if cfg.PGMaxConnIdleTime, err = parseDuration(raw.PGMaxConnIdleTime, defaultMaxConnIdleTime); err != nil {
		return Config{}, fmt.Errorf("parse PG_MAX_CONN_IDLE_TIME: %w", err)
	}
	if cfg.PGHealthPeriod, err = parseDuration(raw.PGHealthPeriod, defaultHealthCheckPeriod); err != nil {
		return Config{}, fmt.Errorf("parse PG_HEALTH_CHECK_PERIOD: %w", err)
	}
	if cfg.OTLPMetricInterval, err = parseDuration(raw.OTLPInterval, defaultOTLPMetricInterval); err != nil {
		return Config{}, fmt.Errorf("parse OTLP_METRIC_INTERVAL: %w", err)
	}

	if cfg.PostgresDSN == "" {
		return Config{}, fmt.Errorf("POSTGRES_DSN is required")
	}
	if cfg.PGMaxConns <= 0 {
		cfg.PGMaxConns = defaultMaxConns
	}
	if cfg.PGMinConns < 0 {
		cfg.PGMinConns = defaultMinConns
	}
	if cfg.PGMinConns > cfg.PGMaxConns {
		cfg.PGMinConns = cfg.PGMaxConns
	}
	if cfg.KafkaEnabled && len(cfg.KafkaBrokers) == 0 {
		return Config{}, fmt.Errorf("KAFKA_BROKERS is required when KAFKA_CONSUMER_ENABLED=true")
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

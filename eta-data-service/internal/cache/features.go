package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/dns/logistics/eta-data-service/internal/service"
)

const (
	redisFeaturesKeyPrefix = "eta:features:shipment:"
)

// FeaturesCache описывает контракт кеша фич по shipment_id.
type FeaturesCache interface {
	GetShipmentFeatures(ctx context.Context, shipmentID string) (service.ShipmentFeatures, bool, error)
	SetShipmentFeatures(ctx context.Context, features service.ShipmentFeatures, ttl time.Duration) error
	Close() error
}

// RedisFeaturesCache реализует FeaturesCache поверх Redis.
type RedisFeaturesCache struct {
	client *redis.Client
}

// NewRedisFeaturesCache создаёт кеш фич поверх redis.Options.
func NewRedisFeaturesCache(addr string, db int) *RedisFeaturesCache {
	return &RedisFeaturesCache{
		client: redis.NewClient(&redis.Options{
			Addr: addr,
			DB:   db,
		}),
	}
}

func (c *RedisFeaturesCache) key(shipmentID string) (string, error) {
	if shipmentID == "" {
		return "", fmt.Errorf("empty shipmentID")
	}
	return redisFeaturesKeyPrefix + shipmentID, nil
}

// GetShipmentFeatures возвращает фичи и флаг hit.
func (c *RedisFeaturesCache) GetShipmentFeatures(ctx context.Context, shipmentID string) (service.ShipmentFeatures, bool, error) {
	var zero service.ShipmentFeatures

	key, err := c.key(shipmentID)
	if err != nil {
		return zero, false, err
	}

	val, err := c.client.Get(ctx, key).Bytes()
	if err != nil {
		if err == redis.Nil {
			return zero, false, nil
		}
		return zero, false, fmt.Errorf("redis GET failed: %w", err)
	}

	var features service.ShipmentFeatures
	if err := json.Unmarshal(val, &features); err != nil {
		return zero, false, fmt.Errorf("unmarshal features: %w", err)
	}

	return features, true, nil
}

// SetShipmentFeatures сохраняет фичи с заданным TTL.
func (c *RedisFeaturesCache) SetShipmentFeatures(ctx context.Context, features service.ShipmentFeatures, ttl time.Duration) error {
	key, err := c.key(features.ShipmentID)
	if err != nil {
		return err
	}

	data, err := json.Marshal(features)
	if err != nil {
		return fmt.Errorf("marshal features: %w", err)
	}

	if err := c.client.Set(ctx, key, data, ttl).Err(); err != nil {
		return fmt.Errorf("redis SET failed: %w", err)
	}

	return nil
}

// Close закрывает соединение с Redis.
func (c *RedisFeaturesCache) Close() error {
	if c == nil || c.client == nil {
		return nil
	}
	return c.client.Close()
}


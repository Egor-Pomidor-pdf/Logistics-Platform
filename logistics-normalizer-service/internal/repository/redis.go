package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const redisEventKeyPrefix = "event:"

// EventDeduplicator filters repeated events by event_id.
type EventDeduplicator interface {
	CheckAndMark(ctx context.Context, eventID string) (isDuplicate bool, err error)
	Close() error
}

// RedisDeduplicator uses Redis SETNX + EXPIRE for idempotency.
type RedisDeduplicator struct {
	client *redis.Client
	ttl    time.Duration
}

// NewRedisDeduplicator creates redis-based dedup storage.
func NewRedisDeduplicator(addr string, ttl time.Duration) *RedisDeduplicator {
	return &RedisDeduplicator{
		client: redis.NewClient(&redis.Options{Addr: addr}),
		ttl:    ttl,
	}
}

func (r *RedisDeduplicator) CheckAndMark(ctx context.Context, eventID string) (bool, error) {
	if eventID == "" {
		return false, fmt.Errorf("eventID is empty")
	}

	key := redisEventKeyPrefix + eventID
	wasSet, err := r.client.SetNX(ctx, key, "1", r.ttl).Result()
	if err != nil {
		return false, fmt.Errorf("redis SETNX failed: %w", err)
	}
	if !wasSet {
		return true, nil
	}

	if err := r.client.Expire(ctx, key, r.ttl).Err(); err != nil {
		return false, fmt.Errorf("redis EXPIRE failed: %w", err)
	}

	return false, nil
}

func (r *RedisDeduplicator) Close() error {
	if r == nil || r.client == nil {
		return nil
	}
	return r.client.Close()
}

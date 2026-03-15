package repository

import (
	"testing"
	"time"
)

func TestNewRedisDeduplicator_ConstructsCorrectly(t *testing.T) {
	ttl := 24 * time.Hour
	d := NewRedisDeduplicator("localhost:6379", ttl)
	if d == nil {
		t.Fatal("expected non-nil deduplicator")
	}
	if d.ttl != ttl {
		t.Fatalf("expected ttl %v, got %v", ttl, d.ttl)
	}
	if d.client == nil {
		t.Fatal("expected non-nil redis client")
	}
	// Clean up the client (no actual connection is made until a command is sent).
	_ = d.Close()
}

func TestRedisDeduplicator_ImplementsInterface(t *testing.T) {
	var _ EventDeduplicator = (*RedisDeduplicator)(nil)
}

func TestRedisDeduplicator_CloseNil(t *testing.T) {
	var d *RedisDeduplicator
	err := d.Close()
	if err != nil {
		t.Fatalf("expected nil error from Close on nil receiver, got %v", err)
	}
}

func TestRedisDeduplicator_CloseNilClient(t *testing.T) {
	d := &RedisDeduplicator{}
	err := d.Close()
	if err != nil {
		t.Fatalf("expected nil error from Close with nil client, got %v", err)
	}
}

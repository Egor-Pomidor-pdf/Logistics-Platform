package state

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store manages export cursors in Postgres.
type Store struct {
	pool         *pgxpool.Pool
	queryTimeout time.Duration
}

// Cursor represents last exported position.
type Cursor struct {
	TS time.Time
	ID string
}

// ErrLockNotAcquired indicates another instance holds the lock.
var ErrLockNotAcquired = fmt.Errorf("export lock not acquired")

// NewStore creates state store.
func NewStore(pool *pgxpool.Pool, queryTimeout time.Duration) *Store {
	return &Store{pool: pool, queryTimeout: queryTimeout}
}

// EnsureSchema creates export state table.
func (s *Store) EnsureSchema(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	const query = `
		CREATE TABLE IF NOT EXISTS analytics_export_state (
			stream TEXT PRIMARY KEY,
			cursor_ts TIMESTAMPTZ NOT NULL,
			cursor_id TEXT NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`

	_, err := s.pool.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("create analytics_export_state: %w", err)
	}
	return nil
}

// GetCursor returns cursor for a stream.
func (s *Store) GetCursor(ctx context.Context, stream string) (Cursor, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	const query = `
		SELECT cursor_ts, cursor_id
		FROM analytics_export_state
		WHERE stream = $1
	`

	var c Cursor
	if err := s.pool.QueryRow(ctx, query, stream).Scan(&c.TS, &c.ID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Cursor{}, false, nil
		}
		return Cursor{}, false, fmt.Errorf("get cursor: %w", err)
	}
	return c, true, nil
}

// UpdateCursor writes cursor for a stream.
func (s *Store) UpdateCursor(ctx context.Context, stream string, cursor Cursor) error {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	const query = `
		INSERT INTO analytics_export_state (stream, cursor_ts, cursor_id, updated_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (stream)
		DO UPDATE SET cursor_ts = EXCLUDED.cursor_ts, cursor_id = EXCLUDED.cursor_id, updated_at = now()
	`

	_, err := s.pool.Exec(ctx, query, stream, cursor.TS.UTC(), cursor.ID)
	if err != nil {
		return fmt.Errorf("update cursor: %w", err)
	}
	return nil
}

// WithAdvisoryLock executes fn under advisory lock.
func (s *Store) WithAdvisoryLock(ctx context.Context, key string, fn func(context.Context) error) error {
	acquired, err := s.tryLock(ctx, key)
	if err != nil {
		return err
	}
	if !acquired {
		return ErrLockNotAcquired
	}
	defer func() {
		_ = s.unlock(ctx, key)
	}()
	return fn(ctx)
}

func (s *Store) tryLock(ctx context.Context, key string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	const query = `SELECT pg_try_advisory_lock(hashtext($1))`
	var ok bool
	if err := s.pool.QueryRow(ctx, query, key).Scan(&ok); err != nil {
		return false, fmt.Errorf("try advisory lock: %w", err)
	}
	return ok, nil
}

func (s *Store) unlock(ctx context.Context, key string) error {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	const query = `SELECT pg_advisory_unlock(hashtext($1))`
	if _, err := s.pool.Exec(ctx, query, key); err != nil {
		return fmt.Errorf("unlock advisory lock: %w", err)
	}
	return nil
}

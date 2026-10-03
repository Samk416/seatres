package db

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func NewPool(ctx context.Context, url string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = 20 // extra requests queue instead of failing
	if v, err := strconv.Atoi(os.Getenv("DB_MAX_CONNS")); err == nil && v > 0 {
		cfg.MaxConns = int32(v)
	}
	cfg.MaxConnLifetime = time.Hour
	cfg.HealthCheckPeriod = 30 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil { // fail fast if DB is unreachable
		pool.Close()
		return nil, err
	}
	return pool, nil
}

// ConnectWithRetry keeps trying for up to `total`, so a cold start where the
// database is still waking up does not crash the process.
func ConnectWithRetry(url string, total time.Duration) (*pgxpool.Pool, error) {
	deadline := time.Now().Add(total)
	for attempt := 1; ; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		pool, err := NewPool(ctx, url)
		cancel()
		if err == nil {
			return pool, nil
		}
		if time.Now().After(deadline) {
			return nil, err
		}
		slog.Warn("database not ready, retrying", "attempt", attempt, "error", err.Error())
		time.Sleep(2 * time.Second)
	}
}
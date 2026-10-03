package db

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

func NewPool(ctx context.Context, url string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = 20		// Extra request queue instead of failing

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}

	err = pool.Ping(ctx)		// Fail fast if db is unreachable
	if err != nil {
		return nil, err
	}

	return pool, nil
}

// func NewPool(ctx context.Context, url string) (*pgxpool.Pool, error) {
// 	cfg, err := pgxpool.ParseConfig(url)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return pgxpool.NewWithConfig(ctx, cfg)
// }

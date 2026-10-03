package reservations

import (
	"context"
	"errors"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

const maxAttempts = 6

// retryable: Postgres rolled our transaction back for a transient reason
// (40001 serialization failure, 40P01 deadlock), so running it again is safe.
func retryable(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "40001" || pgErr.Code == "40P01"
	}
	return false
}

// withRetry re-runs fn after transient database aborts. Business declines
// (seat taken, limit reached, ...) are returned immediately, never retried.
func withRetry(ctx context.Context, fn func() error) error {
	var err error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		err = fn()
		if err == nil || !retryable(err) {
			return err
		}
		d := time.Duration(5<<attempt) * time.Millisecond   // 5, 10, 20, 40 ms ...
		d += time.Duration(rand.Intn(5)) * time.Millisecond // jitter
		select {
		case <-time.After(d):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return err
}

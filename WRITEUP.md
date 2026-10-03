# Write-up

## Atomic decision
Each seat is a row in `seats` (PK show_id, seat_no). Reserving runs
`UPDATE seats SET status='confirmed', ... WHERE ... AND status='available'` and checks rows affected.
Postgres row-locks the row; a concurrent update waits, re-evaluates the WHERE after the first commits,
and updates 0 rows, which becomes a clean 409. The database decides, not application memory.

## Multi-seat and deadlocks
All-or-nothing in one transaction; any failure rolls back every seat. Seats are sorted before
updating so all transactions take row locks in the same order (no lock cycles). Deadlocks are
additionally retried (40P01/40001) as a safety net.

## Per-user limit
A per-user advisory lock (pg_advisory_xact_lock) serializes one user's requests, then the count
is checked inside the same transaction. Other users are never blocked.

## Idempotency
The key and a SHA-256 of (show, sorted seats) are stored on the reservation row in the SAME
transaction as the seat updates, with UNIQUE(user_id, idempotency_key). Same key + same hash
returns the original reservation (Idempotent-Replay header). Same key + different hash returns 409.

## Holds and expiry
Explicit cancel only (owner-only). Cancel locks the reservation row and releases only seats that
still point at that reservation_id, so a stale cancel can never free a seat re-booked by someone
else. The `held` state exists in the schema but is unused.

## Consistency vs availability
Single Postgres primary: consistency over availability. If the DB is unreachable, /readyz returns
503 and reserve requests fail rather than guess. No seat can be oversold during a partition.

## Observability
Metrics: reservations_confirmed_total, reservations_declined_total{reason}, seats_available
(read from the DB at scrape time), http_requests_total, latency histogram, db_pool_*. Structured
JSON logs with request_id. I'd page on: 5xx rate, p99 latency, /readyz failing, and
db_pool_acquired_conns near max with rising db_pool_empty_acquires_total.

## Measured
Local: 20,000-user burst, zero 5xx, invariants held. Live free tier: correctness checks pass,
zero 5xx at 2,000 users; throughput about 70 req/s (CPU-bound free instance), so a 20,000
simultaneous burst may exceed the 30s request timeout and return 503.

## AI usage
I used Claude as a step-by-step guide. I directed the design (Postgres as the single arbiter) and
ran and tested every step myself, including breaking things on purpose (removing the sort, the
advisory lock) to see the failures. AI wrote the first drafts of most code and tests; I debugged
the failures (Windows connection limits, test isolation, a 500 on bad JSON) and can explain each part.

## Next
Hold expiry with a TTL, map DB-down errors to 503, load shedding / rate limiting, real identity
provider, CI, and a paid instance for the burst.
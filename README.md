# seatres: seat reservation at scale

Go + Fiber + Postgres. Live: https://seatres.onrender.com

## Run locally
    docker compose up --build        # app on :8080, Postgres on :5432
Schema is created automatically on startup (embedded migrations).

## Burst script (hot-seat storm + on-sale stampede + limit/ownership + reconciliation)
    bash burst.sh http://localhost:8080
    ADMIN_TOKEN=your-admin-token bash burst.sh https://seatres.onrender.com
Flags: -users -seats -hot-users -hot-seats -concurrency (default 200 in flight).
Prints the outcome distribution (confirmed / declined by reason / 5xx) and the final reconciliation.

## API
- POST /auth/token {"user_id":"alice"}  -> JWT (stand-in for a real login)
- POST /shows (admin: Authorization: Bearer <ADMIN_TOKEN>) {"name","seats":[...],"price_paise"}
- POST /shows/{id}/reserve (user JWT) {"seats":[...],"idempotency_key":"..."}  (or Idempotency-Key header)
- POST /reservations/{id}/cancel (owner only)
- GET /shows/{id}
- GET /healthz (liveness), /readyz (checks DB, 503 if down), /metrics (Prometheus)

Partial requests are all-or-nothing. Declines are 409 (seat_taken, per_user_limit,
idempotency_key_conflict). The admin token is sent separately.

## Notes
- Free-tier hosting: the service sleeps after 15 minutes idle (first request can take about a minute); please call /healthz first.
- Free-tier CPU is small (about 70 req/s measured). A very large burst will queue.
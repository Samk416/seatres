CREATE TABLE IF NOT EXISTS shows (
  id             UUID PRIMARY KEY,
  name           TEXT NOT NULL,
  price_paise    BIGINT NOT NULL CHECK (price_paise >= 0),
  per_user_limit INT NOT NULL DEFAULT 4,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS reservations (
  id              UUID PRIMARY KEY,
  show_id         UUID NOT NULL REFERENCES shows(id),
  user_id         TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  request_hash    TEXT NOT NULL,
  amount_paise    BIGINT NOT NULL,
  status          TEXT NOT NULL CHECK (status IN ('confirmed','cancelled')),
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (user_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS seats (
  show_id        UUID NOT NULL REFERENCES shows(id),
  seat_no        TEXT NOT NULL,
  status         TEXT NOT NULL DEFAULT 'available'
                 CHECK (status IN ('available','held','confirmed')),
  user_id        TEXT,
  reservation_id UUID REFERENCES reservations(id),
  PRIMARY KEY (show_id, seat_no)
);

CREATE INDEX IF NOT EXISTS idx_seats_user ON seats (show_id, user_id);
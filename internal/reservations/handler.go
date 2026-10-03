package reservations

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Samk416/seatres/internal/auth"
	"github.com/Samk416/seatres/internal/metrics"
	"github.com/Samk416/seatres/internal/validate"
)

const maxSeatsPerRequest = 20

type Handler struct {
	DB *pgxpool.Pool
}

type reserveReq struct {
	Seats          []string `json:"seats"`
	IdempotencyKey string   `json:"idempotency_key"`
}

type result struct {
	ReservationID string   `json:"reservation_id"`
	ShowID        string   `json:"show_id"`
	UserID        string   `json:"user_id"`
	Seats         []string `json:"seats"`
	AmountPaise   int64    `json:"amount_paise"`
	Status        string   `json:"status"`
}

// decline is a normal "no" answer (4xx). It is never a server error.
type decline struct {
	Status int
	Reason string
	Msg    string
}

func (d *decline) Error() string { return d.Msg }

func bad(c *fiber.Ctx, msg string) error {
	return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "bad_request", "message": msg})
}

// normalizeSeats validates the list and sorts it so that every
// transaction locks seats in the same order (prevents deadlocks).
func normalizeSeats(in []string) ([]string, string) {
	if len(in) == 0 {
		return nil, "seats must not be empty"
	}
	if len(in) > maxSeatsPerRequest {
		return nil, fmt.Sprintf("at most %d seats per request", maxSeatsPerRequest)
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if s == "" || !validate.Text(s, 32) {
			return nil, "seat names must be 1 to 32 valid characters"
		}
		if _, dup := seen[s]; dup {
			return nil, "duplicate seat in request: " + s
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	sort.Strings(out)
	return out, ""
}

// requestHash fingerprints the meaning of a request (show + sorted seats).
func requestHash(showID uuid.UUID, seats []string) string {
	sum := sha256.Sum256([]byte(showID.String() + "|" + strings.Join(seats, ",")))
	return hex.EncodeToString(sum[:])
}

// POST /shows/:id/reserve
func (h *Handler) Reserve(c *fiber.Ctx) error {
	showID, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return bad(c, "invalid show id")
	}

	var req reserveReq
	if err := c.BodyParser(&req); err != nil {
		return bad(c, "invalid JSON")
	}
	key := strings.TrimSpace(c.Get("Idempotency-Key"))
	if key == "" {
		key = strings.TrimSpace(req.IdempotencyKey)
	}
	if key == "" || !validate.Text(key, 200) {
		return bad(c, "idempotency key required (1-200 valid characters; Idempotency-Key header or idempotency_key field)")
	}
	seats, msg := normalizeSeats(req.Seats)
	if msg != "" {
		return bad(c, msg)
	}

	// Identity comes ONLY from the verified token.
	userID := auth.UserID(c)

	var res *result
	var replayed bool
	err = withRetry(c.UserContext(), func() error {
		var e error
		res, replayed, e = h.reserve(c.UserContext(), showID, userID, key, seats)
		return e
	})
	var d *decline
	if errors.As(err, &d) {
		metrics.Declined.WithLabelValues(d.Reason).Inc()
		c.Locals("reason", d.Reason) // shows up in the request log line
		return c.Status(d.Status).JSON(fiber.Map{"error": d.Reason, "message": d.Msg})
	}
	if err != nil {
		return err
	}
	// Counted once, after any retries have finished.
	if replayed {
		metrics.Declined.WithLabelValues("idempotent_replay").Inc()
		c.Locals("reason", "idempotent_replay")
		c.Set("Idempotent-Replay", "true")
	} else {
		metrics.Confirmed.Inc()
	}
	return c.Status(fiber.StatusCreated).JSON(res)
}

// reserve returns (result, replayed, error). replayed=true means the key
// was already used by an identical request and the original is returned.
func (h *Handler) reserve(ctx context.Context, showID uuid.UUID, userID, key string, seats []string) (*result, bool, error) {
	tx, err := h.DB.Begin(ctx)
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback(ctx) // undoes everything unless we Commit

	hash := requestHash(showID, seats)

	// 1. Serialize all of this user's reservations. Released at commit/rollback.
	_, err = tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1::text, 0))`,
		"user:"+userID)
	if err != nil {
		return nil, false, err
	}

	// 2. Idempotency check: has this user already used this key?
	var prev result
	var prevHash string
	err = tx.QueryRow(ctx,
		`SELECT id::text, show_id::text, user_id, request_hash, amount_paise, status, seats
		   FROM reservations
		  WHERE user_id = $1 AND idempotency_key = $2`,
		userID, key).Scan(&prev.ReservationID, &prev.ShowID, &prev.UserID, &prevHash,
		&prev.AmountPaise, &prev.Status, &prev.Seats)
	if err == nil {
		if prevHash != hash {
			return nil, false, &decline{409, "idempotency_key_conflict",
				"this idempotency key was already used with a different request"}
		}
		return &prev, true, nil // exact retry: return the original, change nothing
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, false, err
	}

	// 3. New request: load the show.
	var price int64
	var limit int
	err = tx.QueryRow(ctx,
		`SELECT price_paise, per_user_limit FROM shows WHERE id = $1`,
		showID.String()).Scan(&price, &limit)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, &decline{404, "show_not_found", "show not found"}
	}
	if err != nil {
		return nil, false, err
	}

	// 4. Per-user limit (safe to count: this user's other requests are blocked).
	var held int
	err = tx.QueryRow(ctx,
		`SELECT count(*) FROM seats
		  WHERE show_id = $1 AND user_id = $2 AND status IN ('held','confirmed')`,
		showID.String(), userID).Scan(&held)
	if err != nil {
		return nil, false, err
	}
	if held+len(seats) > limit {
		return nil, false, &decline{409, "per_user_limit",
			fmt.Sprintf("limit is %d seats per user for this show; you already hold %d", limit, held)}
	}

	// 5. Record the key + request in the SAME transaction as the seat updates.
	resID := uuid.New()
	amount := price * int64(len(seats))
	_, err = tx.Exec(ctx,
		`INSERT INTO reservations
		   (id, show_id, user_id, idempotency_key, request_hash, amount_paise, status, seats)
		 VALUES ($1, $2, $3, $4, $5, $6, 'confirmed', $7)`,
		resID.String(), showID.String(), userID, key, hash, amount, seats)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" { // should not happen: lock serializes us
			return nil, false, &decline{409, "idempotency_key_conflict", "idempotency key conflict, retry"}
		}
		return nil, false, err
	}

	// 6. The atomic decision, one seat at a time, in sorted order.
	for _, s := range seats {
		tag, err := tx.Exec(ctx,
			`UPDATE seats
			    SET status = 'confirmed', user_id = $3, reservation_id = $4
			  WHERE show_id = $1 AND seat_no = $2 AND status = 'available'`,
			showID.String(), s, userID, resID.String())
		if err != nil {
			return nil, false, err
		}
		if tag.RowsAffected() == 0 {
			var st string
			err := tx.QueryRow(ctx,
				`SELECT status FROM seats WHERE show_id = $1 AND seat_no = $2`,
				showID.String(), s).Scan(&st)
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, false, &decline{404, "unknown_seat", "seat " + s + " does not exist in this show"}
			}
			if err != nil {
				return nil, false, err
			}
			return nil, false, &decline{409, "seat_taken", "seat " + s + " is not available"}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, false, err
	}
	return &result{
		ReservationID: resID.String(),
		ShowID:        showID.String(),
		UserID:        userID,
		Seats:         seats,
		AmountPaise:   amount,
		Status:        "confirmed",
	}, false, nil
}

// POST /reservations/:id/cancel
func (h *Handler) Cancel(c *fiber.Ctx) error {
	resID, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return bad(c, "invalid reservation id")
	}
	userID := auth.UserID(c) // identity from the token only

	var res *result
	err = withRetry(c.UserContext(), func() error {
		var e error
		res, e = h.cancel(c.UserContext(), resID, userID)
		return e
	})
	var d *decline
	if errors.As(err, &d) {
		return c.Status(d.Status).JSON(fiber.Map{"error": d.Reason, "message": d.Msg})
	}
	if err != nil {
		return err
	}
	return c.JSON(res)
}

func (h *Handler) cancel(ctx context.Context, resID uuid.UUID, userID string) (*result, error) {
	tx, err := h.DB.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	// Lock the reservation row: concurrent cancels of it queue up here.
	var res result
	err = tx.QueryRow(ctx,
		`SELECT id::text, show_id::text, user_id, amount_paise, status, seats
		   FROM reservations WHERE id = $1 FOR UPDATE`,
		resID.String()).Scan(&res.ReservationID, &res.ShowID, &res.UserID,
		&res.AmountPaise, &res.Status, &res.Seats)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, &decline{404, "reservation_not_found", "reservation not found"}
	}
	if err != nil {
		return nil, err
	}

	if res.UserID != userID {
		return nil, &decline{403, "forbidden", "you can only cancel your own reservations"}
	}

	// Already cancelled: succeed without touching anything (idempotent).
	if res.Status == "cancelled" {
		return &res, nil
	}

	if _, err = tx.Exec(ctx,
		`UPDATE reservations SET status = 'cancelled' WHERE id = $1`,
		resID.String()); err != nil {
		return nil, err
	}

	// Only seats that still belong to THIS reservation are released.
	if _, err = tx.Exec(ctx,
		`UPDATE seats
		    SET status = 'available', user_id = NULL, reservation_id = NULL
		  WHERE reservation_id = $1 AND status = 'confirmed'`,
		resID.String()); err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	res.Status = "cancelled"
	return &res, nil
}

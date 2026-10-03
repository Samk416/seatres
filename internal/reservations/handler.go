package reservations

import (
	"context"
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
		if s == "" {
			return nil, "seat name must not be empty"
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
	if key == "" {
		return bad(c, "idempotency key required (Idempotency-Key header or idempotency_key field)")
	}
	seats, msg := normalizeSeats(req.Seats)
	if msg != "" {
		return bad(c, msg)
	}

	// Identity comes ONLY from the verified token.
	userID := auth.UserID(c)

	res, err := h.reserve(c.UserContext(), showID, userID, key, seats)
	var d *decline
	if errors.As(err, &d) {
		return c.Status(d.Status).JSON(fiber.Map{"error": d.Reason, "message": d.Msg})
	}
	if err != nil {
		return err
	}
	return c.Status(fiber.StatusCreated).JSON(res)
}

func (h *Handler) reserve(ctx context.Context, showID uuid.UUID, userID, key string, seats []string) (*result, error) {
	tx, err := h.DB.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) // undoes everything unless we Commit

	var price int64
	err = tx.QueryRow(ctx, `SELECT price_paise FROM shows WHERE id = $1`, showID.String()).Scan(&price)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, &decline{404, "show_not_found", "show not found"}
	}
	if err != nil {
		return nil, err
	}

	resID := uuid.New()
	amount := price * int64(len(seats))

	// Reservation row first: seats.reservation_id points at it.
	// request_hash is filled in properly in Step 6.
	_, err = tx.Exec(ctx,
		`INSERT INTO reservations (id, show_id, user_id, idempotency_key, request_hash, amount_paise, status)
		 VALUES ($1, $2, $3, $4, $5, $6, 'confirmed')`,
		resID.String(), showID.String(), userID, key, "", amount)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" { // unique violation
			return nil, &decline{409, "idempotency_key_reused", "idempotency key already used (replay handling arrives in Step 6)"}
		}
		return nil, err
	}

	// The atomic decision, one seat at a time, in sorted order.
	for _, s := range seats {
		tag, err := tx.Exec(ctx,
			`UPDATE seats
			    SET status = 'confirmed', user_id = $3, reservation_id = $4
			  WHERE show_id = $1 AND seat_no = $2 AND status = 'available'`,
			showID.String(), s, userID, resID.String())
		if err != nil {
			return nil, err
		}
		if tag.RowsAffected() == 0 {
			// Either the seat does not exist, or somebody else has it.
			var st string
			err := tx.QueryRow(ctx,
				`SELECT status FROM seats WHERE show_id = $1 AND seat_no = $2`,
				showID.String(), s).Scan(&st)
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, &decline{404, "unknown_seat", "seat " + s + " does not exist in this show"}
			}
			if err != nil {
				return nil, err
			}
			return nil, &decline{409, "seat_taken", "seat " + s + " is not available"}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &result{
		ReservationID: resID.String(),
		ShowID:        showID.String(),
		UserID:        userID,
		Seats:         seats,
		AmountPaise:   amount,
		Status:        "confirmed",
	}, nil
}
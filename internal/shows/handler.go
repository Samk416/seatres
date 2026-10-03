package shows

import (
	"errors"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"

	"github.com/Samk416/seatres/internal/validate"
)

const defaultPerUserLimit = 4 // private package level variable

type Handler struct {
	DB *pgxpool.Pool
}

type createReq struct {
	Name       string   `json:"name"`
	Seats      []string `json:"seats"`
	PricePaise int64    `json:"price_paise"`
}

type seatView struct {
	SeatNo string `json:"seat_no"`
	Status string `json:"status"`
}

func badRequest(c *fiber.Ctx, msg string) error {
	return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"err": msg})
}

// POST/shows

func (h *Handler) Create(c *fiber.Ctx) error {
	var req createReq
	if err := c.BodyParser(&req); err != nil {
		return badRequest(c, "invalid JSON")
	}

	req.Name = strings.TrimSpace(req.Name)
	req.Name = strings.TrimSpace(req.Name)
	if req.Name == "" || !validate.Text(req.Name, 200) {
		return badRequest(c, "name is required (max 200 characters)")
	}
	if len(req.Seats) == 0 || len(req.Seats) > 100000 {
		return badRequest(c, "seats must contain 1 to 100000 entries")
	}
	if req.PricePaise < 0 || req.PricePaise > 1_000_000_000_000 {
		return badRequest(c, "price_paise must be between 0 and 1000000000000")
	}
	seen := make(map[string]struct{}, len(req.Seats))
	for _, s := range req.Seats {
		if s == "" || !validate.Text(s, 32) {
			return badRequest(c, "seat names must be 1 to 32 valid characters")
		}
		if _, dup := seen[s]; dup {
			return badRequest(c, "duplicate seat: "+s)
		}
		seen[s] = struct{}{}
	}

	id := uuid.New()
	ctx := c.UserContext()

	tx, err := h.DB.Begin(ctx)
	if err != nil {
		log.Error().Err(err)
		return err
	}
	defer tx.Rollback(ctx) // no-op after a successful commit

	_, err = tx.Exec(ctx, `INSERT INTO shows (id, name, price_paise, per_user_limit) VALUES ($1,$2,$3,$4)`, id.String(), req.Name, req.PricePaise, defaultPerUserLimit)
	if err != nil {
		log.Error().Err(err)
		return err
	}

	_, err = tx.Exec(ctx, `INSERT INTO seats (show_id, seat_no) SELECT $1::uuid,unnest($2::text[])`, id.String(), req.Seats)
	if err != nil {
		log.Error().Err(err)
		return err
	}

	err = tx.Commit(ctx)
	if err != nil {
		log.Error().Err(err)
		return err
	}

	seats := make([]seatView, len(req.Seats))
	for i, s := range req.Seats {
		seats[i] = seatView{SeatNo: s, Status: "available"}
	}

	return c.Status(fiber.StatusCreated).JSON(fiber.Map{
		"id":             id,
		"name":           req.Name,
		"price_paise":    req.PricePaise,
		"per_user_limit": defaultPerUserLimit,
		"seats":          seats,
	})
}

// GET /shows/:id

func (h *Handler) Get(c *fiber.Ctx) error {
	id, err := uuid.Parse(c.Params("id"))
	if err != nil {
		log.Error().Err(err)
		return badRequest(c, "invalid show id")
	}

	ctx := c.UserContext()

	var name string
	var price int64
	var limit int

	err = h.DB.QueryRow(ctx, `SELECT name, price_paise, per_user_limit FROM shows WHERE id = $1`, id.String()).Scan(&name, &price, &limit)

	if errors.Is(err, pgx.ErrNoRows) {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "show not found"})
	}
	if err != nil {
		log.Error().Err(err)
		return err
	}

	rows, err := h.DB.Query(ctx, `SELECT seat_no, status FROM seats WHERE show_id = $1 ORDER BY seat_no`, id.String())

	if err != nil {
		log.Error().Err(err)
		return err
	}

	defer rows.Close()

	counts := map[string]int{"available": 0, "held": 0, "confirmed": 0}
	seats := []seatView{}
	for rows.Next() {
		var s seatView
		if err := rows.Scan(&s.SeatNo, &s.Status); err != nil {
			return err
		}
		seats = append(seats, s)
		counts[s.Status]++
	}
	if err := rows.Err(); err != nil {
		return err
	}

	return c.JSON(fiber.Map{
		"id":             id,
		"name":           name,
		"price_paise":    price,
		"per_user_limit": limit,
		"total_seats":    len(seats),
		"available":      counts["available"],
		"held":           counts["held"],
		"confirmed":      counts["confirmed"],
		"seats":          seats,
	})

}

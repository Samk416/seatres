package server

import (
	"github.com/gofiber/fiber/v2"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Samk416/seatres/internal/auth"
	"github.com/Samk416/seatres/internal/reservations"
	"github.com/Samk416/seatres/internal/shows"
)

func New(pool *pgxpool.Pool, a *auth.Auth) *fiber.App {
	app := fiber.New()
	sh := &shows.Handler{DB: pool}
	rh := &reservations.Handler{DB: pool}

	app.Post("/auth/token", a.IssueToken)
	app.Get("/me", a.RequireUser, func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{"user_id": auth.UserID(c)})
	})
	app.Post("/shows", a.RequireAdmin, sh.Create)
	app.Get("/shows/:id", sh.Get)
	app.Post("/shows/:id/reserve", a.RequireUser, rh.Reserve)
	app.Post("/reservations/:id/cancel", a.RequireUser, rh.Cancel)
	return app
}

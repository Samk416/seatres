package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"time"

	"github.com/gofiber/fiber/v2"
	recovermw "github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Samk416/seatres/internal/auth"
	"github.com/Samk416/seatres/internal/reservations"
	"github.com/Samk416/seatres/internal/shows"
)

const requestTimeout = 30 * time.Second

// errorHandler is the single place where unexpected errors become responses.
// errorHandler is the single place where unexpected errors become responses.
func errorHandler(c *fiber.Ctx, err error) error {
	var fe *fiber.Error
	if errors.As(err, &fe) { // 404 unknown route, 405, 413 body too large, ...
		return c.Status(fe.Code).JSON(fiber.Map{"error": "http_error", "message": fe.Message})
	}
	// Malformed or mistyped JSON body is the client's fault: 400, never 500.
	var syn *json.SyntaxError
	var typ *json.UnmarshalTypeError
	if errors.As(err, &syn) || errors.As(err, &typ) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "bad_request", "message": "request body is not valid JSON for this endpoint"})
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		c.Set("Retry-After", "1")
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error": "overloaded", "message": "request timed out waiting for the database; retry"})
	}
	log.Printf("internal error: %s %s: %v", c.Method(), c.Path(), err)
	return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
		"error": "internal_error", "message": "internal error"})
}

// withTimeout puts a deadline on every request's database work.
func withTimeout(d time.Duration) fiber.Handler {
	return func(c *fiber.Ctx) error {
		ctx, cancel := context.WithTimeout(c.UserContext(), d)
		defer cancel()
		c.SetUserContext(ctx)
		return c.Next()
	}
}

func New(pool *pgxpool.Pool, a *auth.Auth) *fiber.App {
	app := fiber.New(fiber.Config{
		ErrorHandler:          errorHandler,
		BodyLimit:             1 << 20, // 1 MB
		ReadTimeout:           30 * time.Second,
		WriteTimeout:          30 * time.Second,
		IdleTimeout:           60 * time.Second,
		DisableStartupMessage: true,
	})
	app.Use(recovermw.New()) // a panic becomes a clean 500, not a dead connection
	app.Use(withTimeout(requestTimeout))

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

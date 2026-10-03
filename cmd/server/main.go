package main

import (
	"context"
	"log"
	"os"

	"github.com/gofiber/fiber/v2"

	"github.com/Samk416/seatres/internal/auth"
	"github.com/Samk416/seatres/internal/db"
	"github.com/Samk416/seatres/internal/shows"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	ctx := context.Background()

	pool, err := db.NewPool(ctx, env("DATABASE_URL", "postgres://seat:seat@localhost:5432/seatres"))
	if err != nil {
		log.Fatalf("cannot connect to database: %v", err)
	}
	defer pool.Close()

	a := &auth.Auth{
		Secret:     []byte(env("JWT_SECRET", "dev-secret-change-me")),
		AdminToken: env("ADMIN_TOKEN", "admin-dev-token"),
	}
	h := &shows.Handler{DB: pool}

	app := fiber.New()
	app.Post("/auth/token", a.IssueToken)
	app.Get("/me", a.RequireUser, func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{"user_id": auth.UserID(c)})
	})
	app.Post("/shows", a.RequireAdmin, h.Create)
	app.Get("/shows/:id", h.Get)

	log.Fatal(app.Listen(":" + env("PORT", "8080")))
}

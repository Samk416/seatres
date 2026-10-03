package main

import (
	"context"
	"log"
	"os"

	"github.com/gofiber/fiber/v2"

	"github.com/Samk416/seatres/internal/db"
	"github.com/Samk416/seatres/internal/shows"
)

func main() {
	ctx := context.Background()

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://seat:seat@localhost:5432/seatres"
	}
	pool, err := db.NewPool(ctx, dbURL)
	if err != nil {
		log.Fatalf("cannot connect to database: %v", err)
	}
	defer pool.Close()

	app := fiber.New()
	h := &shows.Handler{DB: pool}
	app.Post("/shows", h.Create)
	app.Get("/shows/:id", h.Get)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Fatal(app.Listen(":" + port))
}

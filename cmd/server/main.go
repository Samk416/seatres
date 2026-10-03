package main

import (
	"context"
	"log"
	"os"

	"github.com/Samk416/seatres/internal/auth"
	"github.com/Samk416/seatres/internal/db"
	"github.com/Samk416/seatres/internal/server"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	pool, err := db.NewPool(context.Background(),
		env("DATABASE_URL", "postgres://seat:seat@localhost:5432/seatres"))
	if err != nil {
		log.Fatalf("cannot connect to database: %v", err)
	}
	defer pool.Close()

	a := &auth.Auth{
		Secret:     []byte(env("JWT_SECRET", "dev-secret-change-me")),
		AdminToken: env("ADMIN_TOKEN", "admin-dev-token"),
	}
	app := server.New(pool, a)
	log.Fatal(app.Listen(":" + env("PORT", "8080")))
}

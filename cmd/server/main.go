package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

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

	a := &auth.Auth{
		Secret:     []byte(env("JWT_SECRET", "dev-secret-change-me")),
		AdminToken: env("ADMIN_TOKEN", "admin-dev-token"),
	}
	app := server.New(pool, a)
	port := env("PORT", "8080")

	go func() {
		if err := app.Listen(":" + port); err != nil {
			log.Fatalf("listen: %v", err)
		}
	}()
	log.Printf("listening on :%s", port)

	// Wait for Ctrl+C (or SIGTERM from the hosting platform), then drain.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	log.Println("shutting down: finishing in-flight requests")
	_ = app.ShutdownWithTimeout(15 * time.Second)
	pool.Close()
}
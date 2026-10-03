package main

import (
	"context"
	"log/slog"
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
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	pool, err := db.ConnectWithRetry(
		env("DATABASE_URL", "postgres://seat:seat@localhost:5432/seatres"), 90*time.Second)
	if err != nil {
		slog.Error("cannot connect to database", "error", err.Error())
		os.Exit(1)
	}

	migCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	err = db.Migrate(migCtx, pool)
	cancel()
	if err != nil {
		slog.Error("migration failed", "error", err.Error())
		os.Exit(1)
	}

	a := &auth.Auth{
		Secret:     []byte(env("JWT_SECRET", "dev-secret-change-me")),
		AdminToken: env("ADMIN_TOKEN", "admin-dev-token"),
	}
	app := server.New(pool, a)
	port := env("PORT", "8080")

	go func() {
		if err := app.Listen(":" + port); err != nil {
			slog.Error("listen failed", "error", err.Error())
			os.Exit(1)
		}
	}()
	slog.Info("listening", "port", port)

	// Wait for Ctrl+C (or SIGTERM from the hosting platform), then drain.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	slog.Info("shutting down: finishing in-flight requests")
	_ = app.ShutdownWithTimeout(15 * time.Second)
	pool.Close()
}
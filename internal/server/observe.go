package server

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"

	"github.com/Samk416/seatres/internal/auth"
	"github.com/Samk416/seatres/internal/metrics"
)

const requestIDKey = "request_id"

// validRequestID accepts a caller-supplied id only if it is short and harmless.
func validRequestID(s string) bool {
	if s == "" || len(s) > 64 {
		return false
	}
	for _, r := range s {
		ok := r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' ||
			r == '-' || r == '_' || r == '.'
		if !ok {
			return false
		}
	}
	return true
}

// observe assigns a request id, runs the request, then records metrics
// and writes one structured log line.
func observe(known map[string]bool) fiber.Handler {
	return func(c *fiber.Ctx) error {
		start := time.Now()
		rid := c.Get("X-Request-ID")
		if !validRequestID(rid) {
			rid = uuid.NewString()
		}
		c.Locals(requestIDKey, rid)
		c.Set("X-Request-ID", rid)

		if err := c.Next(); err != nil {
			// Run the error handler here so we see (and log) the final status.
			if herr := c.App().ErrorHandler(c, err); herr != nil {
				_ = c.SendStatus(fiber.StatusInternalServerError)
			}
		}

		route := c.Route().Path
		if !known[route] {
			route = "unmatched" // keeps metric label cardinality bounded
		}
		status := c.Response().StatusCode()
		elapsed := time.Since(start)

		metrics.HTTPRequests.WithLabelValues(route, strconv.Itoa(status)).Inc()
		metrics.HTTPDuration.WithLabelValues(route).Observe(elapsed.Seconds())

		if route == "/healthz" || route == "/readyz" || route == "/metrics" {
			return nil // probes would drown the real logs
		}
		attrs := []slog.Attr{
			slog.String("request_id", rid),
			slog.String("method", c.Method()),
			slog.String("route", route),
			slog.String("path", c.Path()),
			slog.Int("status", status),
			slog.Float64("duration_ms", float64(elapsed.Microseconds())/1000),
		}
		if u := auth.UserID(c); u != "" {
			attrs = append(attrs, slog.String("user_id", u))
		}
		if r, _ := c.Locals("reason").(string); r != "" {
			attrs = append(attrs, slog.String("reason", r))
		}
		level := slog.LevelInfo
		if status >= 500 {
			level = slog.LevelError
		}
		slog.LogAttrs(context.Background(), level, "request", attrs...)
		return nil
	}
}

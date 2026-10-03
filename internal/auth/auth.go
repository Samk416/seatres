package auth

import (
	"crypto/subtle"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/golang-jwt/jwt/v5"
)

const userKey = "user_id"

type Auth struct {
	Secret     []byte
	AdminToken string
}

func bearer(c *fiber.Ctx) string {
	h := c.Get("Authorization")
	if !strings.HasPrefix(h, "Bearer ") {
		return ""
	}
	return strings.TrimSpace(h[len("Bearer "):])
}

// parseUser verifies the signature and returns the user id inside the token.
func (a *Auth) parseUser(raw string) (string, bool) {
	var claims jwt.RegisteredClaims
	tok, err := jwt.ParseWithClaims(raw, &claims,
		func(t *jwt.Token) (any, error) { return a.Secret, nil },
		jwt.WithValidMethods([]string{"HS256"}), // blocks the "alg: none" trick
	)
	if err != nil || !tok.Valid || claims.Subject == "" {
		return "", false
	}
	return claims.Subject, true
}

func (a *Auth) isAdmin(raw string) bool {
	return subtle.ConstantTimeCompare([]byte(raw), []byte(a.AdminToken)) == 1
}

func unauthorized(c *fiber.Ctx) error {
	return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "missing or invalid token"})
}

// Token mints a signed token for a user id.
func (a *Auth) Token(userID string) (string, error) {
	now := time.Now()
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.RegisteredClaims{
		Subject:   userID,
		IssuedAt:  jwt.NewNumericDate(now),
		ExpiresAt: jwt.NewNumericDate(now.Add(7 * 24 * time.Hour)),
	})
	return tok.SignedString(a.Secret)
}

// POST /auth/token  {"user_id": "alice"}
func (a *Auth) IssueToken(c *fiber.Ctx) error {
	var req struct {
		UserID string `json:"user_id"`
	}
	if err := c.BodyParser(&req); err != nil || strings.TrimSpace(req.UserID) == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "user_id is required"})
	}
	s, err := a.Token(req.UserID)
	if err != nil {
		return err
	}
	return c.JSON(fiber.Map{"token": s, "user_id": req.UserID})
}

// Middleware: any valid user. Stores the user id for the handler.
func (a *Auth) RequireUser(c *fiber.Ctx) error {
	uid, ok := a.parseUser(bearer(c))
	if !ok {
		return unauthorized(c)
	}
	c.Locals(userKey, uid)
	return c.Next()
}

// Middleware: admin only.
func (a *Auth) RequireAdmin(c *fiber.Ctx) error {
	raw := bearer(c)
	if a.isAdmin(raw) {
		return c.Next()
	}
	if _, ok := a.parseUser(raw); ok {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": "admin only"})
	}
	return unauthorized(c)
}

// UserID returns the authenticated user's id inside a handler.
func UserID(c *fiber.Ctx) string {
	s, _ := c.Locals(userKey).(string)
	return s
}
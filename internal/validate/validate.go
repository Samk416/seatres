package validate

import (
	"strings"
	"unicode/utf8"
)

// Text reports whether s is safe to store: valid UTF-8, no NUL byte
// (Postgres rejects NUL in text, which would surface as a 500),
// and at most max bytes.
func Text(s string, max int) bool {
	return len(s) <= max && utf8.ValidString(s) && !strings.ContainsRune(s, 0)
}

// Package migrations embeds the SQL files so the binary can create its own schema.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
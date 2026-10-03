#!/usr/bin/env bash
# One-command burst against a running service.
#   ./burst.sh http://localhost:8080
#   ADMIN_TOKEN=<token> ./burst.sh https://<your-live-url>
set -euo pipefail
cd "$(dirname "$0")"
exec go run ./burst -base "${1:-${BASE_URL:-http://localhost:8080}}"
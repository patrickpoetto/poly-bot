#!/bin/sh
set -e

if echo "${DATABASE_URL:-}" | grep -q "^postgresql"; then
  echo "Waiting for Postgres..."
  until pg_isready -h "${POSTGRES_HOST:-postgres}" -U "${POSTGRES_USER:-poly_bot}" >/dev/null 2>&1; do
    sleep 2
  done
  echo "Running migrations..."
  poly-bot ops migrate
fi

exec "$@"

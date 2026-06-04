#!/usr/bin/env bash
# Manage poly_bot Docker stacks without saturating home bandwidth.
# Usage: ./docker/stack.sh weather-only | stop-heavy | status | on discovery|heavy|ops

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

ENV_FILE="${ENV_FILE:-.env}"
PROD="-f docker/docker-compose.prod.yml"
WEATHER="-f docker/docker-compose.weather.yml"

dc() {
  docker compose --env-file "$ENV_FILE" "$@"
}

cmd="${1:-help}"
shift || true

case "$cmd" in
  weather-only|lite)
    echo "Stopping bandwidth-heavy services…"
    dc $PROD stop ws-daemon catalog-watch discovery alerter reconcile backup 2>/dev/null || true
    echo "Starting postgres + dashboard only…"
    dc $PROD up -d postgres dashboard
    dc $PROD ps
    echo ""
    echo "Weather UI: http://127.0.0.1:8765/weather"
    echo "Prices update on city pages only (REST, ~5s). No ws-daemon."
    ;;
  stop-heavy)
    dc $PROD stop ws-daemon catalog-watch discovery 2>/dev/null || true
    echo "Stopped: ws-daemon, catalog-watch, discovery"
    dc $PROD ps
    ;;
  stop-all)
    dc $PROD down
    echo "All prod services stopped."
    ;;
  status|ps)
    dc $PROD ps -a
    ;;
  on)
    svc="${1:?Usage: stack.sh on discovery|catalog-watch|ws-daemon|alerter|…}"
    shift
    profile=""
    case "$svc" in
      discovery) profile="discovery" ;;
      ws-daemon|catalog-watch) profile="heavy" ;;
      alerter|backup|reconcile) profile="ops" ;;
    esac
    if [[ -n "$profile" ]]; then
      dc $PROD --profile "$profile" up -d "$svc" "$@"
    else
      dc $PROD up -d "$svc" "$@"
    fi
    dc $PROD ps
    ;;
  off)
    svc="${1:?Usage: stack.sh off SERVICE}"
    dc $PROD stop "$svc"
    ;;
  sync-catalog)
    echo "One-shot discovery + weather sync (heavy; then stops)…"
    dc $PROD --profile discovery run --rm discovery poly-bot discovery serve --once
    dc $PROD run --rm discovery poly-bot weather sync
    echo "Done."
    ;;
  help|*)
    cat <<'EOF'
poly_bot stack helper (save home bandwidth)

  ./docker/stack.sh weather-only   # stop heavy + start postgres + dashboard
  ./docker/stack.sh stop-heavy     # stop ws-daemon, catalog-watch, discovery
  ./docker/stack.sh status         # docker ps
  ./docker/stack.sh on discovery   # start one service (with profile)
  ./docker/stack.sh off ws-daemon
  ./docker/stack.sh sync-catalog   # one-shot Gamma + weather tables

See docker/STACK.md for details.
EOF
    ;;
esac

#!/usr/bin/env bash
# Launch the whole mock fleet locally with uvicorn (no Docker needed).
# Stage 1 is intentionally runnable this way so you can inspect it fast.
#
#   ./run_local.sh          # start all four services
#   ./run_local.sh stop     # stop them
#
# Logs are written to ./logs/<service>.log (also streamed to each proc's stdout,
# captured in ./logs/<service>.out).

set -euo pipefail
cd "$(dirname "$0")"

export LOG_DIR="${LOG_DIR:-$(pwd)/logs}"
mkdir -p "$LOG_DIR"
PIDFILE="$LOG_DIR/.pids"

if [[ "${1:-start}" == "stop" ]]; then
  if [[ -f "$PIDFILE" ]]; then
    while read -r pid; do kill "$pid" 2>/dev/null || true; done < "$PIDFILE"
    rm -f "$PIDFILE"
    echo "stopped fleet"
  fi
  exit 0
fi

: > "$PIDFILE"

start() {  # name module port
  local name="$1" module="$2" port="$3"
  uvicorn "$module" --host 0.0.0.0 --port "$port" \
    > "$LOG_DIR/$name.out" 2>&1 &
  echo $! >> "$PIDFILE"
  echo "  $name  -> http://localhost:$port  (pid $!)"
}

echo "starting fleet (LOG_DIR=$LOG_DIR):"
start auth       auth.main        8001
start trip       trip.main        8002
start payments   payments.main    8003
# Gateway points at the others via env (defaults already match these ports).
start api-gateway api_gateway.main 8000

echo
echo "try:  curl -s -XPOST localhost:8000/rides/request -H 'content-type: application/json' -d '{\"user_id\":\"u1\"}' | jq"
echo "logs: tail -f $LOG_DIR/*.log"
echo "stop: ./run_local.sh stop"

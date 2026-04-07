#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$SCRIPT_DIR"

cleanup() {
  echo ""
  echo "Shutting down..."
  docker compose down 2>/dev/null
  exit 0
}
trap cleanup SIGINT SIGTERM

echo "Starting Mosquitto..."
docker compose up -d

echo "Waiting for Mosquitto to be ready..."
for i in $(seq 1 10); do
  if docker compose exec -T mosquitto mosquitto_pub -t test -m "" 2>/dev/null; then
    break
  fi
  sleep 0.5
done

echo "Starting publisher + subscriber..."
npm run dev:all

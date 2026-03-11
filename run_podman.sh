#!/usr/bin/env bash
set -euo pipefail

# Get the directory of this script (ensures it works even if run from another location)
BASE_DIR="$(cd "$(dirname "$0")" && pwd)"

IMAGE_NAME="financedb"
CONTAINER_NAME="financedb-job"
DB_DIR="$BASE_DIR/db"
LOGS_DIR="$BASE_DIR/logs"

mkdir -p "$DB_DIR"
mkdir -p "$LOGS_DIR"

podman rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true

podman run --rm \
  --name "$CONTAINER_NAME" \
  -v "$DB_DIR:/app/db:Z" \
  "$IMAGE_NAME"

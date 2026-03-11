#!/usr/bin/env bash
set -euo pipefail

# Get the directory of this script
BASE_DIR="$(cd "$(dirname "$0")" && pwd)"

IMAGE_NAME="financedb"

echo "Building Podman image: $IMAGE_NAME"

podman build \
  -t "$IMAGE_NAME" \
  "$BASE_DIR"

echo "Podman image built successfully."

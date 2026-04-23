#!/usr/bin/env bash
# Start Dagster dev with a stable DAGSTER_HOME so materializations persist
# across restarts. Run from anywhere — the script finds its own directory.

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

if [ ! -d ".venv" ]; then
  echo ">> .venv not found — creating and installing deps"
  uv venv
  uv pip install -e .
fi

source .venv/bin/activate

export DAGSTER_HOME="$PROJECT_DIR/.dagster_home"
mkdir -p "$DAGSTER_HOME"

# Sync instance config from the tracked template so DAGSTER_HOME can stay
# gitignored while dagster.yaml lives in source control.
cp "$PROJECT_DIR/dagster.yaml" "$DAGSTER_HOME/dagster.yaml"

echo ">> DAGSTER_HOME=$DAGSTER_HOME"
echo ">> UI will be at http://localhost:3000"
echo

exec dagster dev "$@"

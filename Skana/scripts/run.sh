#!/usr/bin/env bash
# --------------------------------------------------------------------------
# run.sh — Launch TeleopClient with one YAML config (may list multiple cameras).
#
# Usage:
#   ./scripts/run.sh                    # uses config/config.yaml
#   ./scripts/run.sh --config path.yaml # explicit single config file
#   TELEOP_LOG_LEVEL=DEBUG ./scripts/run.sh
# --------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

if [ -f .venv/bin/activate ]; then
    # shellcheck disable=SC1091
    source .venv/bin/activate
fi

if [[ ! " $* " =~ " --config " ]] && [ -f config/config.yaml ]; then
    exec python3 -m teleop_client --config config/config.yaml "$@"
else
    exec python3 -m teleop_client "$@"
fi

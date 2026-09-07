#!/bin/bash
set -euo pipefail

# Resolve the repo root relative to this script, regardless of caller's cwd
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$REPO_ROOT"
source venv/bin/activate

echo "=== Run started at $(date) ==="
python3 -u scripts/main_2.py
echo "=== Run finished at $(date) ==="

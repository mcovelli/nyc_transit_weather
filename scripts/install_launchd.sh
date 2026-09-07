#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
LABEL="com.nyctransitweather.pipeline"
PLIST_TEMPLATE="$SCRIPT_DIR/${LABEL}.plist"
PLIST_DEST="$HOME/Library/LaunchAgents/${LABEL}.plist"

chmod +x "$SCRIPT_DIR/run_pipeline.sh"

mkdir -p "$HOME/Library/LaunchAgents"
sed "s#__REPO_ROOT__#${REPO_ROOT}#g" "$PLIST_TEMPLATE" > "$PLIST_DEST"

launchctl unload "$PLIST_DEST" 2>/dev/null || true
launchctl load -w "$PLIST_DEST"

echo "Installed and loaded ${LABEL}."
echo "It will run scripts/main_2.py at 6:00 AM and 6:00 PM every day."
echo "Logs: ${REPO_ROOT}/pipeline_cron.log"

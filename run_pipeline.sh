#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

YYMMDD=$(date +%y%m%d)
HHMM=$(date +%H%M)
LOG_DIR="output/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="${LOG_DIR}/${YYMMDD}_${HHMM}_pipeline.log"

exec > >(tee -a "$LOG_FILE") 2>&1

stamp() { date -u "+%Y-%m-%dT%H:%M:%SZ"; }

echo "[$(stamp)] === YSS pipeline starting ==="
echo "[$(stamp)] Log: $LOG_FILE"

python3 fetch_intraday.py

# If fetch exited 0 but wrote no TSV, market was closed — clean no-op
if ! ls output/*_minute_bars.tsv 2>/dev/null | grep -q .; then
  echo "[$(stamp)] No minute bars TSV found — market closed or no data. Pipeline no-op."
  exit 0
fi

python3 compute_vwap.py
python3 validate_yahoo.py || echo "[$(stamp)] validate_yahoo.py failed — skipping (dashboard will not show validation warning)"
python3 render_page.py
python3 prune_logs.py

echo "[$(stamp)] === Pipeline complete ==="

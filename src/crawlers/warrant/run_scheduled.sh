#!/usr/bin/env bash
# cron entry point for update.sh.
#
#   run_scheduled.sh [daily|full]
#
# cron runs with a near-empty environment and no venv, so everything the job
# needs is pinned here rather than inherited: the interpreter, and a timestamp
# on each run so a log of many runs stays readable. Paths default to the NFS
# locations inside the package, so no data_sdk env vars are needed; set
# DATA_SDK_WARRANT_CACHE_PATH here if that ever changes.
set -euo pipefail

MODE="${1:-daily}"
PACKAGE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$PACKAGE_DIR/../../.." && pwd)"

export PYTHON="$REPO_DIR/.venv/bin/python"
if [[ ! -x "$PYTHON" ]]; then
    echo "no interpreter at $PYTHON -- create it with:" >&2
    echo "  python3 -m venv $REPO_DIR/.venv && $REPO_DIR/.venv/bin/pip install -e $REPO_DIR" >&2
    exit 1
fi

echo "===== $(date '+%Y-%m-%d %H:%M:%S')  warrant update ($MODE) ====="
bash "$PACKAGE_DIR/update.sh" "$MODE"
echo "===== $(date '+%Y-%m-%d %H:%M:%S')  done ====="

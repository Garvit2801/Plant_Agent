#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   PROJECT_ID="my-plant-agent-123456" BQ_LOCATION="asia-south2" ./sql/backfill_suggestions.sh

PROJECT_ID="${PROJECT_ID:?Set PROJECT_ID}"
BQ_LOCATION="${BQ_LOCATION:-asia-south2}"

# Resolve script dir and SQL file
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_FILE="${SCRIPT_DIR}/backfill/backfill_suggestions_from_routine.sql"

if [[ ! -f "$SQL_FILE" ]]; then
  echo "❌ SQL file not found at: $SQL_FILE"
  echo "   Expected the file generated earlier: sql/backfill/backfill_suggestions_from_routine.sql"
  exit 1
fi

echo "➡️  Running backfill using project=$PROJECT_ID, location=$BQ_LOCATION"
# IMPORTANT: we pipe the SQL via stdin so leading '--' comments are NOT treated as flags.
bq --location="$BQ_LOCATION" --project_id="$PROJECT_ID" query --nouse_legacy_sql < "$SQL_FILE"

echo "✅ Backfill completed. Sample rows:"
bq --location="$BQ_LOCATION" --project_id="$PROJECT_ID" query --nouse_legacy_sql -- \
"SELECT created_at, lever, current_value, proposed_value
 FROM \`plant_ops.suggestions_v1\`
 ORDER BY created_at DESC
 LIMIT 10"

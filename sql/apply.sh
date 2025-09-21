#!/usr/bin/env bash
set -euo pipefail

: "${PROJECT_ID:?Set PROJECT_ID}"
BQ_LOCATION="${BQ_LOCATION:-asia-south2}"

echo "Project: $PROJECT_ID   Location: $BQ_LOCATION"
bq --location="$BQ_LOCATION" --project_id="$PROJECT_ID" mk --dataset plant_ops || true

apply_dir () {
  local dir="$1"
  if [ -d "$dir" ]; then
    # Run files in lexical order (01_, 02_, …)
    for f in "$dir"/*.sql; do
      [ -e "$f" ] || continue
      echo "Applying $f"
      bq --location="$BQ_LOCATION" --project_id="$PROJECT_ID" query --nouse_legacy_sql < "$f"
    done
  fi
}

# Order matters: tables → UDFs → views
apply_dir "sql/ddl"
apply_dir "sql/views"

echo "Done."

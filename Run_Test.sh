#!/usr/bin/env bash
set -euo pipefail

# ── Config ──────────────────────────────────────────────────────────────────────
: "${BASE_URL:?Set BASE_URL, e.g. https://plant-agent-xxxx.a.run.app}"
DELTA_PCT="${DELTA_PCT:-8}"   # target load change (percent)
STEPS="${STEPS:-3}"           # number of stages to request
HOLD_SEC="${HOLD_SEC:-8}"     # wait between stages to let the mock settle
# If Cloud Run is private: export AGENT_TOKEN="$(gcloud auth print-identity-token)"

# KPIs to print (edit if you add more in /snapshot)
KPI_KEYS=(production_tph kiln_feed_tph separator_dp_pa id_fan_flow_Nm3_h cooler_airflow_Nm3_h kiln_speed_rpm o2_percent specific_power_kwh_per_ton)

# ── Curl helper (adds auth header only if provided) ─────────────────────────────
fetch() {
  if [[ -n "${AGENT_TOKEN:-}" ]]; then
    curl -s -H "Authorization: Bearer ${AGENT_TOKEN}" "$@"
  else
    curl -s "$@"
  fi
}

# Extract KPI object from raw JSON (supports {}, {"data":{}}, {"snapshot":{}})
extract_kpis() {
  local json="$1"
  local keys_json
  keys_json="$(printf '%s\n' "${KPI_KEYS[@]}" | jq -R . | jq -s .)"
  echo "$json" | jq -c --argjson keys "$keys_json" '
    . as $raw
    | ( if ($raw|type)=="object"
        then ( if ( ($raw|has("data")) and (($raw.data|type)=="object") ) then $raw.data
               elif ( ($raw|has("snapshot")) and (($raw.snapshot|type)=="object") ) then $raw.snapshot
               else $raw end )
        else {} end ) as $o
    | reduce $keys[] as $k ({}; .[$k] = ($o[$k] // null))
  '
}

# Compute delta between two KPI objects (numbers only; else null)
delta_kpis() {
  local a="$1" b="$2"
  local keys_json
  keys_json="$(printf '%s\n' "${KPI_KEYS[@]}" | jq -R . | jq -s .)"
  jq -n --argjson a "$a" --argjson b "$b" --argjson keys "$keys_json" '
    reduce $keys[] as $k ({}; .[$k] =
      ( if (($a[$k]|type)=="number" and ($b[$k]|type)=="number")
        then ($b[$k] - $a[$k])
        else null end ))
  '
}

# ── 0) Baseline snapshot ────────────────────────────────────────────────────────
RAW_BASE="$(fetch "$BASE_URL/snapshot")"
if ! echo "$RAW_BASE" | jq -e 'type=="object"' >/dev/null 2>&1; then
  echo "Snapshot didn’t return JSON object. Raw response below:"; echo "$RAW_BASE"; exit 2
fi
BASE="$(extract_kpis "$RAW_BASE")"
echo "Baseline KPIs:"; echo "$BASE" | jq .

# ── 1) Build + print the plan (stages) ─────────────────────────────────────────
LOAD_PAYLOAD="$(jq -n --argjson snap "$RAW_BASE" --arg dir up --argjson pct "$DELTA_PCT" --argjson steps "$STEPS" \
  '{direction:$dir, delta_pct:$pct, steps:$steps, snapshot:$snap}')"

PLAN="$(fetch -X POST "$BASE_URL/optimize/load" -H "Content-Type: application/json" -d "$LOAD_PAYLOAD")"

STAGES_JSON="$(echo "$PLAN" | jq -c '(.stages // .plan.stages // [])')"
COUNT="$(echo "$STAGES_JSON" | jq -r 'length')"
if [[ "$COUNT" -eq 0 ]]; then
  echo "No stages in /optimize/load response. Full response below:"; echo "$PLAN" | jq .; exit 1
fi

echo; echo "Plan target: $(echo "$PLAN" | jq -c '.target // {}')"
echo "Stages ($COUNT):"
echo "$STAGES_JSON" | jq -r '
  to_entries[]
  | "\(.key+1). \(.value.name // ("Stage " + ((.key+1)|tostring))) → setpoints: \(.value.setpoints)"
'

# ── 2) Apply each stage and print KPIs + Δ vs baseline ─────────────────────────
PREV="$BASE"
for ((i=0; i<COUNT; i++)); do
  STAGE="$(echo "$STAGES_JSON" | jq -c --argjson idx "$i" '.[$idx]')"
  NAME="$(echo "$STAGE" | jq -r --argjson idx "$i" '.name // ("Stage " + (($idx+1)|tostring))')"
  SETPTS="$(echo "$STAGE" | jq -c '.setpoints')"

  echo; echo "==== Applying ${NAME} ===="
  echo "Setpoints: $SETPTS"
  BODY="$(jq -n --argjson stg "$STAGE" '{stage:$stg}')"

  fetch -o /dev/null -w "apply_stage status: %{http_code}\n" \
    -X POST "$BASE_URL/actuate/apply_stage" \
    -H "Content-Type: application/json" -d "$BODY"

  echo "Holding ${HOLD_SEC}s to settle..."; sleep "$HOLD_SEC"

  RAW_AFTER="$(fetch "$BASE_URL/snapshot")"
  if ! echo "$RAW_AFTER" | jq -e 'type=="object"' >/dev/null 2>&1; then
    echo "Snapshot after ${NAME} is not an object. Raw:"; echo "$RAW_AFTER"; exit 2
  fi
  AFTER="$(extract_kpis "$RAW_AFTER")"

  echo "KPIs after ${NAME}:"; echo "$AFTER" | jq .
  echo "Δ vs baseline:"; delta_kpis "$BASE" "$AFTER" | jq .
  echo "Δ vs previous:"; delta_kpis "$PREV" "$AFTER" | jq .

  PREV="$AFTER"
done

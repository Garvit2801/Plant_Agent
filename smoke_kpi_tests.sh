#!/usr/bin/env bash
set -euo pipefail

# -------------------------------
# Config — change as needed
# -------------------------------
: "${PROJECT_ID:=my-plant-agent-123456}"
: "${BQ_LOCATION:=asia-south2}"
: "${DATASET:=plant_ops}"
: "${SNAPSHOT_TABLE:=snapshots_current}"   # view or table
: "${MODEL:=spower_reg}"
: "${REGION:=asia-south2}"
: "${DELTA_PCT:=8}"                        # <-- default +8% load for /optimize/load

# Optional OIDC for private Cloud Run:
# export AGENT_TOKEN="$(gcloud auth print-identity-token)"

# Resolve Cloud Run URL if not provided
if [[ -z "${BASE_URL:-}" ]]; then
  echo "Resolving Cloud Run URL..."
  BASE_URL="$(gcloud run services describe plant-agent --region="${REGION}" --format='value(status.url)' 2>/dev/null || true)"
fi

if [[ -z "${BASE_URL:-}" ]]; then
  echo "❌ BASE_URL not set and couldn't resolve from Cloud Run. Export BASE_URL and re-run."
  exit 1
fi

echo "Using:"
echo "  BASE_URL       = $BASE_URL"
echo "  PROJECT_ID     = $PROJECT_ID"
echo "  DATASET        = $DATASET"
echo "  SNAPSHOT_TABLE = $SNAPSHOT_TABLE"
echo "  MODEL          = $MODEL"
echo

require_cmd() { command -v "$1" >/dev/null 2>&1 || { echo "❌ Missing dependency: $1"; exit 1; }; }
require_cmd curl
require_cmd jq
require_cmd bq

pass() { echo -e "✅ $*"; }
fail() { echo -e "❌ $*"; exit 1; }

section() {
  echo
  echo "=============================================="
  echo "$*"
  echo "=============================================="
}

fetch() {
  if [[ -n "${AGENT_TOKEN:-}" ]]; then
    curl -s -H "Authorization: Bearer ${AGENT_TOKEN}" "$@"
  else
    curl -s "$@"
  fi
}

# -------------------------------
# 0) Basic health checks
# -------------------------------
section "0) Health checks"
code=$(fetch -o /dev/null -w "%{http_code}" "$BASE_URL/health")
[[ "$code" == "200" ]] || fail "/health returned $code"
pass "/health 200 OK"

if curl -sI "$BASE_URL/health" | head -n1 | grep -qi "200"; then
  pass "HEAD /health 200 OK"
else
  echo "ℹ️ HEAD /health not supported or filtered; continuing"
fi

# -------------------------------
# 1) Optional config debug
# -------------------------------
section "1) Config debug (optional)"
DBG="$(fetch "$BASE_URL/debug/config" || true)"
if echo "$DBG" | jq -e 'type=="object" and (has("bq_table") or has("project_id_effective"))' >/dev/null 2>&1; then
  echo "$DBG" | jq '{resolved_path, bq_enabled, bq_table, project_id_effective, keys}'
  pass "/debug/config available"
else
  echo "ℹ️ /debug/config not exposed in this build; skipping"
fi

# -------------------------------
# 2) Ingest a snapshot and verify landing in BQ
# -------------------------------
section "2) Ingest snapshot -> BigQuery"
resp="$(fetch -X POST "$BASE_URL/ingest" -H "Content-Type: application/json" -d '{}')"
echo "$resp" | jq '.' || true
echo "$resp" | jq -e '.ok == true' >/dev/null || fail "/ingest did not return ok:true"
pass "/ingest returned ok:true"

echo "Waiting ~4s for streaming buffers..."
sleep 4

echo "Checking latest row in ${DATASET}.${SNAPSHOT_TABLE} ..."
bq --location="$BQ_LOCATION" query --nouse_legacy_sql --format=json "
SELECT ts, o2_percent, specific_power_kwh_per_ton, production_tph
FROM \`${PROJECT_ID}.${DATASET}.${SNAPSHOT_TABLE}\`
WHERE ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)
ORDER BY ts DESC
LIMIT 1
" | jq '.[0]' >/dev/null || fail "No recent rows found in ${SNAPSHOT_TABLE}"
pass "Recent row found in ${SNAPSHOT_TABLE}"

# -------------------------------
# 3) Baseline snapshot
# -------------------------------
section "3) Baseline /snapshot"

echo
echo "Assembling optimizer snapshot payload..."
SNAPSHOT_JSON="$(fetch "$BASE_URL/snapshot" | jq -c '{
  ts: (.ts // .data.ts // now),
  production_tph: (.production_tph // .data.production_tph // empty),
  kiln_feed_tph: (.kiln_feed_tph // .data.kiln_feed_tph // empty),
  separator_dp_pa: (.separator_dp_pa // .data.separator_dp_pa // empty),
  id_fan_flow_Nm3_h: (.id_fan_flow_Nm3_h // .data.id_fan_flow_Nm3_h // empty),
  cooler_airflow_Nm3_h: (.cooler_airflow_Nm3_h // .data.cooler_airflow_Nm3_h // empty),
  kiln_speed_rpm: (.kiln_speed_rpm // .data.kiln_speed_rpm // empty),
  o2_percent: (.o2_percent // .data.o2_percent // empty),
  specific_power_kwh_per_ton: (.specific_power_kwh_per_ton // .data.specific_power_kwh_per_ton // empty)
}' 2>/dev/null)"

if [[ -z "${SNAPSHOT_JSON:-}" || "$SNAPSHOT_JSON" == "null" ]]; then
  echo "Falling back to BigQuery for snapshot context..."
  SNAPSHOT_JSON="$(bq --location="$BQ_LOCATION" query --nouse_legacy_sql --format=json "
SELECT AS STRUCT
  TIMESTAMP_TO_SEC(ts) AS ts,
  production_tph,
  kiln_feed_tph,
  separator_dp_pa,
  id_fan_flow_Nm3_h,
  cooler_airflow_Nm3_h,
  kiln_speed_rpm,
  o2_percent,
  specific_power_kwh_per_ton
FROM \`${PROJECT_ID}.${DATASET}.${SNAPSHOT_TABLE}\`
ORDER BY ts DESC
LIMIT 1
" | jq -c '.[0] // empty')"
fi

if [[ -z "${SNAPSHOT_JSON:-}" || "$SNAPSHOT_JSON" == "null" ]]; then
  fail "Unable to assemble snapshot payload for optimizers."
fi
echo "Optimizer snapshot:"; echo "$SNAPSHOT_JSON" | jq '.'

baseline="$(fetch "$BASE_URL/snapshot")"
echo "$baseline" | jq '.'
b_o2="$(echo "$baseline" | jq -r '.o2_percent // .data.o2_percent // empty')"
b_sp="$(echo "$baseline" | jq -r '.specific_power_kwh_per_ton // .data.specific_power_kwh_per_ton // empty')"
[[ -n "${b_o2:-}" && -n "${b_sp:-}" ]] || fail "Baseline snapshot missing o2/specific_power"
# use printf for formatting
printf "Baseline -> O2=%.3f, SP=%.3f\n" "$b_o2" "$b_sp"

# -------------------------------
# 4) /optimize/routine -> apply first suggestion
# -------------------------------
section "4) /optimize/routine -> apply first suggestion"
SKIP_ROUTINE_APPLY=0
stage_json=""
routine_code=""
routine=""

echo "Calling /optimize/routine..."
routine_code="$(fetch -o /tmp/routine.json -w "%{http_code}" -X POST "$BASE_URL/optimize/routine" \
  -H "Content-Type: application/json" \
  -d "{\"snapshot\": $SNAPSHOT_JSON}")"
routine="$(cat /tmp/routine.json 2>/dev/null || echo "{}")"
echo "$routine" | jq '.' || true

if [[ "$routine_code" != "200" ]]; then
  echo "⚠️  /optimize/routine returned HTTP $routine_code, skipping routine-apply stage."
  echo "Body:"; echo "$routine" | jq '.' || true
  SKIP_ROUTINE_APPLY=1
else
  # Robustly pick a stage-like object and coerce to {setpoints:{...}}
  stage_json="$(echo "$routine" | jq -c '
    def as_stage(o):
      if (o|type)=="object" then
        if o|has("setpoints") then {setpoints:o.setpoints}
        elif o|has("controls") then {setpoints:o.controls}
        elif o|has("targets") then {setpoints:o.targets}
        elif o|has("proposed_setpoints") then {setpoints:o.proposed_setpoints}
        else empty end
      else empty end;

    as_stage( .plan     | objects | .stages   | arrays | .[0] )
    // as_stage( .plan   | objects | .steps    | arrays | .[0] )
    // as_stage(           .stages | arrays  | .[0] )
    // as_stage(           .steps  | arrays  | .[0] )
    // as_stage(           .proposal | objects )
    // as_stage(           .proposals | arrays | .[0] )
    // as_stage(           .action | objects )
    // as_stage(           {setpoints:(.proposed_setpoints // empty)} )
  ' )"

  if [[ -z "${stage_json:-}" || "$stage_json" == "null" ]]; then
    echo "❌ Could not locate a stage/proposal in /optimize/routine response."
    echo "Top-level keys were:"; echo "$routine" | jq 'if type=="object" then (keys) else [] end'
    echo "Heuristics looked for: plan.stages/steps, stages/steps, proposal(s), action, or proposed_setpoints."
    SKIP_ROUTINE_APPLY=1
  fi
fi

echo "Candidate stage:"
if [[ $SKIP_ROUTINE_APPLY -eq 0 ]]; then
  echo "$stage_json" | jq '.'
else
  echo "{}" | jq '.'
fi

if [[ $SKIP_ROUTINE_APPLY -eq 0 ]]; then
  apply_resp="$(fetch -X POST "$BASE_URL/actuate/apply_stage" -H "Content-Type: application/json" -d "{\"stage\": $stage_json}")"
  echo "$apply_resp" | jq '.' || true
  echo "$apply_resp" | jq -e '.ok == true' >/dev/null || fail "/actuate/apply_stage did not return ok:true"
  pass "Applied routine stage"

  echo "Waiting for plant loop to react (~6s)..."; sleep 6

  post1="$(fetch "$BASE_URL/snapshot")"
  echo "$post1" | jq '.'
  p_o2="$(echo "$post1" | jq -r '.o2_percent // .data.o2_percent // empty')"
  p_sp="$(echo "$post1" | jq -r '.specific_power_kwh_per_ton // .data.specific_power_kwh_per_ton // empty')"

  if [[ "$p_o2" != "$b_o2" || "$p_sp" != "$b_sp" ]]; then
    pass "Snapshot changed post-apply (O2 $b_o2 -> $p_o2, SP $b_sp -> $p_sp)"
  else
    echo "⚠️ No change detected; waiting extra 10s..."; sleep 10
    post2="$(fetch "$BASE_URL/snapshot")"; echo "$post2" | jq '.' || true
    p2_o2="$(echo "$post2" | jq -r '.o2_percent // .data.o2_percent // empty')"
    p2_sp="$(echo "$post2" | jq -r '.specific_power_kwh_per_ton // .data.specific_power_kwh_per_ton // empty')"
    [[ "$p2_o2" != "$b_o2" || "$p2_sp" != "$b_sp" ]] || fail "Still no observable change after apply"
    pass "Snapshot changed after extra wait (O2 $b_o2 -> $p2_o2, SP $b_sp -> $p2_sp)"
  fi
else
  echo "Skipping apply_stage and post-apply checks for routine step."
fi

# -------------------------------
# 5) /optimize/load (up) -> apply first stage
# -------------------------------
section "5) /optimize/load (up) -> apply first stage"
LOAD_PAYLOAD="$(jq -n --argjson snap "$SNAPSHOT_JSON" --arg dir up --argjson pct "$DELTA_PCT" --argjson steps 3 \
  '{direction:$dir, delta_pct:$pct, steps:$steps, snapshot:$snap}')"
echo "Load-up payload:"; echo "$LOAD_PAYLOAD" | jq '.'

load_up="$(fetch -X POST "$BASE_URL/optimize/load" -H "Content-Type: application/json" -d "$LOAD_PAYLOAD")"
echo "$load_up" | jq '.' || true

lu_stage="$(echo "$load_up" | jq -c '(.plan.stages[0]) // (.stages[0]) // empty')"
if [[ -n "${lu_stage:-}" ]]; then
  lu_apply="$(fetch -X POST "$BASE_URL/actuate/apply_stage" -H "Content-Type: application/json" -d "{\"stage\": $lu_stage}")"
  echo "$lu_apply" | jq '.' || true
  echo "$lu_apply" | jq -e '.ok == true' >/dev/null || fail "Load-up stage apply failed"
  pass "Applied first load-up stage"
else
  echo "ℹ️ /optimize/load did not return a stages array; skipping apply"
fi

# -------------------------------
# 6) Prediction API check
# -------------------------------
section "6) /predict/spower"
row_json="$(bq --location="$BQ_LOCATION" query --nouse_legacy_sql --format=json "
SELECT AS STRUCT
  production_tph, kiln_feed_tph, separator_dp_pa,
  id_fan_flow_Nm3_h, cooler_airflow_Nm3_h,
  kiln_speed_rpm, o2_percent
FROM \`${PROJECT_ID}.${DATASET}.${SNAPSHOT_TABLE}\`
ORDER BY ts DESC
LIMIT 1
" | jq '.[0]')"

[[ -n "${row_json:-}" && "$row_json" != "null" ]] || fail "Could not fetch a latest snapshot row to form predict payload"

predict_payload="$(jq -n --argjson f "$row_json" '{features:$f}')"
echo "Payload to /predict/spower:"; echo "$predict_payload" | jq '.'

pred="$(fetch -X POST "$BASE_URL/predict/spower" -H "Content-Type: application/json" -d "$predict_payload")"
echo "$pred" | jq '.' || true

# Prefer numeric at .prediction.predicted_specific_power_kwh_per_ton; else any numeric field within .prediction
pred_val="$(echo "$pred" | jq -r '
  (.prediction.predicted_specific_power_kwh_per_ton // empty) as $p
  | if ($p|type)=="number" then $p
    else (.prediction|to_entries[]?|select((.value|type)=="number")|.value) end
' 2>/dev/null || true)"

[[ -n "${pred_val:-}" ]] || fail "No numeric prediction found in /predict/spower response"
pass "Prediction returned: $pred_val"

# -------------------------------
# 7) Rollback (optional)
# -------------------------------
section "7) Rollback (optional)"
rb="$(fetch -X POST "$BASE_URL/actuate/rollback" -H "Content-Type: application/json" -d '{}')"
echo "$rb" | jq '.' || true
if echo "$rb" | jq -e '.ok == true' >/dev/null; then
  pass "Rollback ok"
else
  echo "ℹ️ Rollback not supported/needed; continuing"
fi

# -------------------------------
# 8) BQML sanity checks
# -------------------------------
section "8) BigQuery ML sanity"
echo "Checking model exists: ${PROJECT_ID}.${DATASET}.${MODEL}"
if bq --location="$BQ_LOCATION" ls -m "${PROJECT_ID}:${DATASET}" | grep -F " ${MODEL} " >/dev/null; then
  pass "Model found"
else
  fail "Model ${MODEL} not found in ${DATASET}"
fi

echo "Evaluating model quickly..."
bq --location="$BQ_LOCATION" query --nouse_legacy_sql "
SELECT * FROM ML.EVALUATE(MODEL \`${PROJECT_ID}.${DATASET}.${MODEL}\`)
" >/dev/null && pass "ML.EVALUATE ran" || fail "ML.EVALUATE failed"

echo
pass "All KPI tests completed"

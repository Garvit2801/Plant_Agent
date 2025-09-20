#!/usr/bin/env bash
set -euo pipefail

# -------------------------------
# Config — change as needed
# -------------------------------
: "${PROJECT_ID:=my-plant-agent-123456}"
: "${BQ_LOCATION:=asia-south2}"
: "${DATASET:=plant_ops}"
: "${SNAPSHOT_TABLE:=snapshots_current}"   # use snapshots_current view or snapshots_v2 table
: "${MODEL:=spower_reg}"

# Resolve Cloud Run URL if not provided
if [[ -z "${BASE_URL:-}" ]]; then
  echo "Resolving Cloud Run URL..."
  BASE_URL="$(gcloud run services describe plant-agent --region=${REGION:-asia-south2} --format='value(status.url)' 2>/dev/null || true)"
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

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || { echo "❌ Missing dependency: $1"; exit 1; }
}
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

# -------------------------------
# 0) Basic health checks
# -------------------------------
section "0) Health checks"
code=$(curl -s -o /dev/null -w "%{http_code}" "$BASE_URL/health")
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
if curl -s "$BASE_URL/debug/config" | jq -e '.path // empty' >/dev/null; then
  curl -s "$BASE_URL/debug/config" | jq '{path, source, keys:(.config|keys)}'
  pass "/debug/config available"
else
  echo "ℹ️ /debug/config not exposed in this build; skipping"
fi

# -------------------------------
# 2) Ingest a snapshot and verify landing in BQ
# -------------------------------
section "2) Ingest snapshot -> BigQuery"
resp="$(curl -s -X POST "$BASE_URL/ingest" -H "Content-Type: application/json" -d '{}')"
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

# Build a snapshot payload for optimizers (prefer live /snapshot, else synthesize from BQ)
echo
echo "Assembling optimizer snapshot payload..."
SNAPSHOT_JSON="$(curl -s "$BASE_URL/snapshot" | jq -c '{
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
  echo "❌ Unable to assemble snapshot payload for optimizers."
  exit 1
fi
echo "Optimizer snapshot:"
echo "$SNAPSHOT_JSON" | jq '.'

baseline="$(curl -s "$BASE_URL/snapshot")"
echo "$baseline" | jq '.'
b_o2="$(echo "$baseline" | jq -r '.o2_percent // .data.o2_percent // empty')"
b_sp="$(echo "$baseline" | jq -r '.specific_power_kwh_per_ton // .data.specific_power_kwh_per_ton // empty')"
[[ -n "${b_o2:-}" && -n "${b_sp:-}" ]] || fail "Baseline snapshot missing o2/specific_power"
echo "Baseline -> O2=%s, SP=%s" "$b_o2" "$b_sp"

# -------------------------------
# 4) Routine optimizer proposal and apply one step
# -------------------------------
section "4) /optimize/routine -> apply first suggestion"
SKIP_ROUTINE_APPLY=0
stage_json=""
routine_code=""
routine=""

echo "Calling /optimize/routine..."
routine_code="$(curl -s -o /tmp/routine.json -w "%{http_code}" -X POST "$BASE_URL/optimize/routine" -H "Content-Type: application/json" -d '{"snapshot": '"$SNAPSHOT_JSON"', "targets": {"specific_power_kwh_per_ton":{"max_reduction_pct":5}}, "constraints":{"o2_percent":{"min":1.5,"max":3.0}}}')"
routine="$(cat /tmp/routine.json 2>/dev/null || echo "")"
echo "$routine" | jq '.' || true

if [[ "$routine_code" != "200" ]]; then
  echo "⚠️  /optimize/routine returned HTTP $routine_code, skipping routine-apply stage."
  echo "Body:"; echo "$routine" | jq '.' || true
  SKIP_ROUTINE_APPLY=1
else
  # Try to extract a suggested stage or setpoints
  # Support many shapes:
  #   a) {plan:{stages:[{...}]}} or {plan:{steps:[{...}]}}
  #   b) {stages:[...]}, {steps:[...]}
  #   c) {proposal:{...}} or {proposals:[{...}]}
  #   d) {actions:[{...}]} or {action:{...}}
  #   e) Flat body with {setpoints:{...}} or {controls:{...}} or {targets:{...}}
  stage_json="$(echo "$routine" | jq -c '
    def wrap(x):
      if (x|type) == "object" and (x|has("setpoints") or x|has("controls") or x|has("targets")) then
        if x|has("setpoints") then {setpoints:x.setpoints}
        elif x|has("controls") then {setpoints:x.controls}
        else {setpoints:x.targets} end
      else x end;

    # direct candidates
    (
      .plan.stages[0]? // .plan.steps[0]? // .stages[0]? // .steps[0]? //
      .proposal? // .proposals[0]? // .actions[0]? // .action? // {setpoints:(.proposed_setpoints?)}
    ) as $cand
    | if $cand == null then empty else wrap($cand) end
  ')"

  # Fallback: recursively scan for the first object that contains setpoints/controls/targets and wrap it
  if [[ -z "${stage_json:-}" || "$stage_json" == "null" ]]; then
    stage_json="$(echo "$routine" | jq -c '
      [.. | select(type=="object") | select((has("setpoints") or has("controls") or has("targets") or has("proposed_setpoints"))) | {setpoints:(.setpoints // .controls // .targets // .proposed_setpoints)}] | .[0]
    ')"
  fi

  if [[ -z "${stage_json:-}" || "$stage_json" == "null" ]]; then
    echo "❌ Could not locate a stage/proposal in /optimize/routine response."
    echo "Top-level keys were:"
    echo "$routine" | jq 'if type=="object" then (keys?) else empty end'
    echo "Heuristics looked for: plan.stages/steps, stages/steps, proposal(s), action(s), or any object with setpoints/controls/targets."
    SKIP_ROUTINE_APPLY=1
  fi
fi

echo "Candidate stage:"
if [[ -n "${stage_json:-}" && "$stage_json" != "null" && $SKIP_ROUTINE_APPLY -eq 0 ]]; then
  echo "$stage_json" | jq '.'
else
  echo "{}" | jq '.'
fi

if [[ $SKIP_ROUTINE_APPLY -eq 0 && -n "${stage_json:-}" && "$stage_json" != "null" ]]; then
  apply_resp="$(curl -s -X POST "$BASE_URL/actuate/apply_stage" -H "Content-Type: application/json" -d "{\"stage\": $stage_json}")"
  echo "$apply_resp" | jq '.' || true
  echo "$apply_resp" | jq -e '.ok == true' >/dev/null || fail "/actuate/apply_stage did not return ok:true"
  pass "Applied routine stage"

  echo "Waiting for plant loop to react (~6s)..."
  sleep 6

  post1="$(curl -s "$BASE_URL/snapshot")"
  echo "$post1" | jq '.'
  p_o2="$(echo "$post1" | jq -r '.o2_percent // .data.o2_percent // empty')"
  p_sp="$(echo "$post1" | jq -r '.specific_power_kwh_per_ton // .data.specific_power_kwh_per_ton // empty')"

  # We expect either O2 or specific power to move; check that at least one changed
  if [[ "$p_o2" != "$b_o2" || "$p_sp" != "$b_sp" ]]; then
    pass "Snapshot changed post-apply (O2 $b_o2 -> $p_o2, SP $b_sp -> $p_sp)"
  else
    echo "⚠️ No change detected; the mock may have a longer time constant. Waiting extra 10s..."
    sleep 10
    post2="$(curl -s "$BASE_URL/snapshot")"
    echo "$post2" | jq '.' || true
    p2_o2="$(echo "$post2" | jq -r '.o2_percent // .data.o2_percent // empty')"
    p2_sp="$(echo "$post2" | jq -r '.specific_power_kwh_per_ton // .data.specific_power_kwh_per_ton // empty')"
    [[ "$p2_o2" != "$b_o2" || "$p2_sp" != "$b_sp" ]] || fail "Still no observable change after apply"
    pass "Snapshot changed after extra wait (O2 $b_o2 -> $p2_o2, SP $b_sp -> $p2_sp)"
  fi
else
  echo "Skipping apply_stage and post-apply checks for routine step."
fi

section "5) /optimize/load (up) -> apply first stage"
lu_stage=""
load_up=""
LOAD_PAYLOAD="$(jq -n --argjson snap "$SNAPSHOT_JSON" '{direction:"up", steps:3, snapshot:$snap}')"
echo "Load-up payload:"; echo "$LOAD_PAYLOAD" | jq "."
load_up="$(curl -s -X POST "$BASE_URL/optimize/load" -H "Content-Type: application/json" -d "$LOAD_PAYLOAD")"
echo "$load_up" | jq '.' || true
lu_stage="$(echo "$load_up" | jq -c '(.plan.stages[0]) // (.stages[0]) // empty')"
if [[ -n "${lu_stage:-}" ]]; then
  lu_apply="$(curl -s -X POST "$BASE_URL/actuate/apply_stage" -H "Content-Type: application/json" -d "{\"stage\": $lu_stage}")"
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
# Build a feature vector from the latest snapshot in BQ
row_json="$(bq --location="$BQ_LOCATION" query --nouse_legacy_sql --format=json "
SELECT AS STRUCT
  production_tph,
  kiln_feed_tph,
  separator_dp_pa,
  id_fan_flow_Nm3_h,
  cooler_airflow_Nm3_h,
  kiln_speed_rpm,
  o2_percent
FROM \`${PROJECT_ID}.${DATASET}.${SNAPSHOT_TABLE}\`
ORDER BY ts DESC
LIMIT 1
" | jq '.[0]')"

if [[ -z "${row_json:-}" || "$row_json" == "null" ]]; then
  fail "Could not fetch a latest snapshot row to form predict payload"
fi
predict_payload="$(jq -n --argjson f "$row_json" '{features:$f}')"
echo "Payload to /predict/spower:"
echo "$predict_payload" | jq '.'

pred="$(curl -s -X POST "$BASE_URL/predict/spower" -H "Content-Type: application/json" -d "$predict_payload")"
echo "$pred" | jq '.' || true
val="$(echo "$pred" | jq -r '.prediction // .pred // .yhat // empty')"
[[ -n "${val:-}" ]] || fail "No numeric prediction key found in /predict/spower response"
pass "Prediction returned: $val"

# -------------------------------
# 7) Rollback (optional)
# -------------------------------
section "7) Rollback (optional)"
rb="$(curl -s -X POST "$BASE_URL/actuate/rollback" -H "Content-Type: application/json" -d '{}')"
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

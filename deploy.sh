#!/usr/bin/env bash
set -euo pipefail

# -----------------------------
# Config (override via env)
# -----------------------------
PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project)}"
RUN_REGION="${RUN_REGION:-asia-south2}"            # Cloud Run region
SCHED_REGION="${SCHED_REGION:-asia-south1}"        # Cloud Scheduler region
SERVICE="${SERVICE:-plant-agent}"                  # Cloud Run service name

REPO="${REPO:-plant-agent}"                        # Artifact Registry repo name
AR_REGION="${AR_REGION:-$RUN_REGION}"              # Artifact Registry region (e.g., asia-south2)
AR_LOC="$AR_REGION"                                # exact location, no extra prefix
IMAGE_HOST="${AR_LOC}-docker.pkg.dev"              # correct AR hostname
IMAGE="${IMAGE_HOST}/${PROJECT_ID}/${REPO}/${SERVICE}"
TAG="${TAG:-$(date +%Y%m%d-%H%M%S)}"
IMAGE_URI="${IMAGE}:${TAG}"

# Runtime env for the container (tweak as needed)
ENV_PROJECT_ID="${ENV_PROJECT_ID:-$PROJECT_ID}"
ENV_DATASET="${ENV_DATASET:-plant_ops}"
ENV_BQ_LOCATION="${ENV_BQ_LOCATION:-asia-south2}"
ENV_USE_MOCK="${ENV_USE_MOCK:-1}"
ENV_APPLY_ENABLED="${ENV_APPLY_ENABLED:-1}"
ENV_SERVICE_VERSION="${ENV_SERVICE_VERSION:-$TAG}"
ENV_BQ_MODEL_NAME="${ENV_BQ_MODEL_NAME:-spower_reg}"

# Default the snapshots table to the v2 partitioned table if not provided
ENV_BQ_SNAPSHOTS_TABLE="${ENV_BQ_SNAPSHOTS_TABLE:-${PROJECT_ID}.${ENV_DATASET}.snapshots_v2}"
ENV_SKIP_RAW="${ENV_SKIP_RAW:-0}"

# Service accounts
RUN_SA="${RUN_SA:-$(gcloud iam service-accounts list --format='value(email)' \
  --filter="displayName:${SERVICE} OR email:${SERVICE}@" | head -n1 || true)}"
RUN_SA="${RUN_SA:-${PROJECT_ID}-compute@developer.gserviceaccount.com}"

SCHED_SA="${SCHED_SA:-plant-agent-scheduler@${PROJECT_ID}.iam.gserviceaccount.com}"
JOB_NAME="${JOB_NAME:-plant-agent-ingest}"

echo "Project:     $PROJECT_ID"
echo "Run region:  $RUN_REGION"
echo "AR region:   $AR_LOC"
echo "Image host:  $IMAGE_HOST"
echo "Image:       $IMAGE_URI"
echo "Service:     $SERVICE"
echo "Run SA:      $RUN_SA"
echo "Sched SA:    $SCHED_SA"
echo "Job Name:    $JOB_NAME"
echo

# -----------------------------
# Enable services
# -----------------------------
gcloud services enable \
  run.googleapis.com \
  artifactregistry.googleapis.com \
  cloudbuild.googleapis.com \
  bigquery.googleapis.com \
  bigquerydatatransfer.googleapis.com \
  cloudscheduler.googleapis.com \
  iam.googleapis.com \
  logging.googleapis.com

# -----------------------------
# Artifact Registry repo
# -----------------------------
if ! gcloud artifacts repositories describe "$REPO" \
  --location="$AR_LOC" >/dev/null 2>&1; then
  gcloud artifacts repositories create "$REPO" \
    --repository-format=docker \
    --location="$AR_LOC" \
    --description="Container repo for ${SERVICE}"
fi

# -----------------------------
# Build & push
# -----------------------------
gcloud builds submit --tag "$IMAGE_URI"

# -----------------------------
# Deploy Cloud Run
# -----------------------------
ENV_FLAGS=(
  "--set-env-vars=SERVICE_VERSION=${ENV_SERVICE_VERSION}"
  "--set-env-vars=PROJECT_ID=${ENV_PROJECT_ID}"
  "--set-env-vars=DATASET=${ENV_DATASET}"
  "--set-env-vars=BQ_LOCATION=${ENV_BQ_LOCATION}"
  "--set-env-vars=USE_MOCK=${ENV_USE_MOCK}"
  "--set-env-vars=APPLY_ENABLED=${ENV_APPLY_ENABLED}"
  "--set-env-vars=SKIP_RAW=${ENV_SKIP_RAW}"
  "--set-env-vars=BQ_MODEL_NAME=${ENV_BQ_MODEL_NAME}"
  "--set-env-vars=BQ_SNAPSHOTS_TABLE=${ENV_BQ_SNAPSHOTS_TABLE}"
)

gcloud run deploy "$SERVICE" \
  --project="$PROJECT_ID" \
  --region="$RUN_REGION" \
  --image="$IMAGE_URI" \
  --service-account="$RUN_SA" \
  --allow-unauthenticated \
  --platform=managed \
  --memory=1Gi \
  --cpu=1 \
  --concurrency=80 \
  --timeout=300 \
  "${ENV_FLAGS[@]}"

# -----------------------------
# URL
# -----------------------------
BASE_URL="$(gcloud run services describe "$SERVICE" --region="$RUN_REGION" --format='value(status.url)')"
echo "Service URL: $BASE_URL"

# -----------------------------
# Create/refresh view: snapshots_current -> snapshots_v2
# -----------------------------
bq query --use_legacy_sql=false --location="$ENV_BQ_LOCATION" \
"CREATE OR REPLACE VIEW \`${PROJECT_ID}.${ENV_DATASET}.snapshots_current\` AS
 SELECT * FROM \`${PROJECT_ID}.${ENV_DATASET}.snapshots_v2\`;"

# -----------------------------
# Optional: Train (or re-train) the BQML model from snapshots_current
# -----------------------------
if [[ -f "train_spower.sql" ]]; then
  echo "Training model ${ENV_BQ_MODEL_NAME} from train_spower.sql ..."
  # Ensure the SQL references ${PROJECT_ID} placeholders; envsubst will replace them.
  env PROJECT_ID="$PROJECT_ID" envsubst < train_spower.sql | \
    bq query --use_legacy_sql=false --location="$ENV_BQ_LOCATION"
else
  echo "NOTE: train_spower.sql not found; skipping training step."
fi

# -----------------------------
# Ensure Scheduler SA exists & has invoke rights
# -----------------------------
gcloud iam service-accounts describe "$SCHED_SA" >/dev/null 2>&1 || \
  gcloud iam service-accounts create "$(echo "$SCHED_SA" | cut -d@ -f1)" --display-name="Plant Agent Scheduler"

gcloud run services add-iam-policy-binding "$SERVICE" \
  --region="$RUN_REGION" \
  --member="serviceAccount:${SCHED_SA}" \
  --role="roles/run.invoker"

# -----------------------------
# Scheduler job (POST {} to /ingest every 5m, JSON headers)
# -----------------------------
if gcloud scheduler jobs describe "$JOB_NAME" --location="$SCHED_REGION" >/dev/null 2>&1; then
  gcloud scheduler jobs update http "$JOB_NAME" \
    --location="$SCHED_REGION" \
    --schedule="*/5 * * * *" \
    --uri="${BASE_URL}/ingest" \
    --http-method=POST \
    --oidc-service-account-email="${SCHED_SA}" \
    --headers="Content-Type=application/json" \
    --message-body='{}'
else
  gcloud scheduler jobs create http "$JOB_NAME" \
    --location="$SCHED_REGION" \
    --schedule="*/5 * * * *" \
    --uri="${BASE_URL}/ingest" \
    --http-method=POST \
    --oidc-service-account-email="${SCHED_SA}" \
    --headers="Content-Type=application/json" \
    --message-body='{}'
fi

# Trigger once now
gcloud scheduler jobs run "$JOB_NAME" --location="$SCHED_REGION" || true

# -----------------------------
# Smoke tests
# -----------------------------
set +e
echo
echo "Health check:"
curl -fsS "${BASE_URL}/health" && echo " OK" || echo " FAILED"

# Authenticated /predict test via SA impersonation (best-effort)
echo
echo "Predict sample (auth as Scheduler SA):"
gcloud iam service-accounts add-iam-policy-binding "$SCHED_SA" \
  --member="user:$(gcloud config get-value account)" \
  --role="roles/iam.serviceAccountTokenCreator" >/dev/null 2>&1

ID_TOKEN="$(gcloud auth print-identity-token --impersonate-service-account="$SCHED_SA" --audiences="$BASE_URL" 2>/dev/null || true)"

if [[ -n "${ID_TOKEN}" ]]; then
  curl -sS -X POST "${BASE_URL}/predict/spower" \
    -H "Authorization: Bearer ${ID_TOKEN}" \
    -H "Content-Type: application/json" \
    -d '{"snapshot":{"production_tph":120,"kiln_feed_tph":118,"separator_dp_pa":450,"id_fan_flow_Nm3_h":180000,"cooler_airflow_Nm3_h":200000,"kiln_speed_rpm":4.5,"o2_percent":2.1}}' | jq . || true
else
  echo "Skipping auth call (could not mint ID token)."
fi

echo
echo "One-shot ingest:"
curl -sS -X POST "${BASE_URL}/ingest" -H "Content-Type: application/json" -d '{}' | jq . || true

echo
echo "Recent rows (snapshots_v2):"
bq query --use_legacy_sql=false --location="$ENV_BQ_LOCATION" \
"SELECT ts, source, o2_percent
 FROM \`${PROJECT_ID}.${ENV_DATASET}.snapshots_v2\`
 ORDER BY ts DESC LIMIT 5" || true

echo
echo "Done."

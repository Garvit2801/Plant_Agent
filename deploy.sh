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
ENV_BQ_SNAPSHOTS_TABLE="${ENV_BQ_SNAPSHOTS_TABLE:-}"

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
)
if [[ -n "$ENV_BQ_SNAPSHOTS_TABLE" ]]; then
  ENV_FLAGS+=("--set-env-vars=BQ_SNAPSHOTS_TABLE=${ENV_BQ_SNAPSHOTS_TABLE}")
fi

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
# IAM for Scheduler
# -----------------------------
gcloud run services add-iam-policy-binding "$SERVICE" \
  --region="$RUN_REGION" \
  --member="serviceAccount:${SCHED_SA}" \
  --role="roles/run.invoker"

# -----------------------------
# Scheduler job
# -----------------------------
if gcloud scheduler jobs describe "$JOB_NAME" --location="$SCHED_REGION" >/dev/null 2>&1; then
  gcloud scheduler jobs update http "$JOB_NAME" \
    --location="$SCHED_REGION" \
    --schedule="*/5 * * * *" \
    --uri="${BASE_URL}/ingest" \
    --http-method=POST \
    --oidc-service-account-email="${SCHED_SA}"
else
  gcloud scheduler jobs create http "$JOB_NAME" \
    --location="$SCHED_REGION" \
    --schedule="*/5 * * * *" \
    --uri="${BASE_URL}/ingest" \
    --http-method=POST \
    --oidc-service-account-email="${SCHED_SA}"
fi

# -----------------------------
# Smoke tests
# -----------------------------
set +e
echo
echo "Health check:"
curl -fsS "${BASE_URL}/healthz" && echo " OK" || echo " FAILED"

echo
echo "One-shot ingest:"
curl -sS -X POST "${BASE_URL}/ingest" | jq . || true

echo
echo "Done."

# PROJECT_ID="my-plant-agent-123456"
# USER_EMAIL="$(gcloud config get-value account)"
# PROJECT_NUMBER="$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)')"
# DEFAULT_SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"

# gcloud iam service-accounts add-iam-policy-binding "$DEFAULT_SA" \
#   --member="user:${USER_EMAIL}" \
#   --role="roles/iam.serviceAccountUser"

# ./deploy.sh
#!/usr/bin/env bash
set -Eeuo pipefail
trap 'echo "❌ Error on line $LINENO. Exiting."; exit 1' ERR

# ==== CONFIG ====
REGION="asia-south2"
PROJECT_ID="$(gcloud config get-value project 2>/dev/null || true)"
AR_REPO="plant-agent"
IMAGE="agent"
TAG="v$(date +%Y%m%d-%H%M%S)"

# ==== PRECHECKS ====
command -v gcloud >/dev/null || { echo "Missing gcloud"; exit 1; }
[[ -n "${PROJECT_ID}" ]] || { echo "Run: gcloud config set project <PROJECT_ID>"; exit 1; }
gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q . || { echo "Run: gcloud auth login"; exit 1; }

# ==== ENSURE AR REPO EXISTS ====
if ! gcloud artifacts repositories describe "$AR_REPO" --location="$REGION" >/dev/null 2>&1; then
  gcloud artifacts repositories create "$AR_REPO" \
    --repository-format=docker \
    --location="$REGION" \
    --description="Plant agent images"
fi

# Show .gcloudignore header (optional)
gcloud topic gcloudignore | sed -n '1,12p' || true

# ==== BUILD & DEPLOY via cloudbuild.yaml ====
gcloud builds submit \
  --region="$REGION" \
  --config="cloudbuild.yaml" \
  --substitutions=_REGION="$REGION",_REPO="$AR_REPO",_IMAGE="$IMAGE",_SERVICE="plant-agent",_TAG="$TAG"

# ==== VERIFY IMAGE PUBLISHED ====
IMG="asia-south2-docker.pkg.dev/${PROJECT_ID}/${AR_REPO}/${IMAGE}:${TAG}"
echo "✅ Built image: ${IMG}"

# (fixed) simple, valid format for this command
gcloud artifacts docker images list \
  "asia-south2-docker.pkg.dev/${PROJECT_ID}/${AR_REPO}" \
  --include-tags \
  --format='table[box,title="AR Images"](package, version, tags, updateTime)' \
  | sed -n '1,25p' || true

# ==== VERIFY CLOUD RUN DEPLOYMENT ====
BASE_URL="$(gcloud run services describe plant-agent --region="$REGION" --format='value(status.url)')"
[[ -n "$BASE_URL" ]] || { echo "❌ No service URL found"; exit 1; }
echo "🌐 Service URL: $BASE_URL"

# ==== HEALTH PINGS (with /healthz → /health fallback) ====
if command -v curl >/dev/null; then
  echo "→ GET /"
  if command -v jq >/dev/null; then curl -sS "$BASE_URL/" | jq . || true; else curl -sS "$BASE_URL/" || true; fi

  echo "→ GET /healthz (then /health if 404)"
  if ! curl -sS -f "$BASE_URL/healthz" >/dev/null; then
    curl -sS -f "$BASE_URL/health" || echo "health endpoint non-200 (check logs)"
  fi

  echo "→ GET /snapshot"
  if command -v jq >/dev/null; then curl -sS "$BASE_URL/snapshot" | jq . || true; else curl -sS "$BASE_URL/snapshot" || true; fi
else
  echo "curl not installed; skipping health checks."
fi

# ==== RECENT LOGS ====
gcloud run services logs read plant-agent --region="$REGION" --limit=50 || true

echo "🎉 Done."

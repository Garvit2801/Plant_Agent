#!/usr/bin/env bash
set -Eeuo pipefail
trap 'echo "❌ Error on line $LINENO"; exit 1' ERR

# -------- CONFIG (override via env) --------
REGION="${REGION:-asia-south2}"
SERVICE="${SERVICE:-plant-agent}"
AR_REPO="${AR_REPO:-plant-agent}"
IMAGE="${IMAGE:-agent}"
TAG="${TAG:-v$(date +%Y%m%d-%H%M%S)}"

# -------- PRECHECKS --------
command -v gcloud >/dev/null || { echo "gcloud not found"; exit 1; }
PROJECT_ID="$(gcloud config get-value project 2>/dev/null || true)"
[[ -n "${PROJECT_ID}" ]] || { echo "Run: gcloud config set project <PROJECT_ID>"; exit 1; }
gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q . || { echo "Run: gcloud auth login"; exit 1; }

# -------- ENSURE AR REPO EXISTS --------
if ! gcloud artifacts repositories describe "$AR_REPO" --location="$REGION" >/dev/null 2>&1; then
  gcloud artifacts repositories create "$AR_REPO" \
    --repository-format=docker \
    --location="$REGION" \
    --description="Plant agent images"
fi

# -------- BUILD & DEPLOY VIA cloudbuild.yaml --------
gcloud builds submit \
  --region="$REGION" \
  --config="cloudbuild.yaml" \
  --substitutions=_REGION="$REGION",_REPO="$AR_REPO",_IMAGE="$IMAGE",_SERVICE="$SERVICE",_TAG="$TAG"

# -------- SERVICE URL --------
BASE_URL="$(gcloud run services describe "$SERVICE" --region="$REGION" --format='value(status.url)')"
[[ -n "$BASE_URL" ]] || { echo "❌ No service URL found for $SERVICE"; exit 1; }
echo "🌐 Service URL: $BASE_URL"

# -------- SHOW LATEST IMAGES IN AR (simple, valid format) --------
echo "📦 Artifact Registry images (top entries):"
gcloud artifacts docker images list \
  "asia-south2-docker.pkg.dev/${PROJECT_ID}/${AR_REPO}" \
  --include-tags \
  --format='table[box,title="AR Images"](package, version, tags, updateTime)' \
  | sed -n '1,25p' || true

# -------- DISCOVER LIVE REVISION IMAGE (DIGEST OR TAG) --------
REV="$(gcloud run services describe "$SERVICE" --region="$REGION" --format='value(status.latestReadyRevisionName)')"
[[ -n "$REV" ]] || { echo "❌ No latestReadyRevisionName"; exit 1; }

IMG="$(gcloud run revisions describe "$REV" --region="$REGION" --format='value(spec.containers[0].image)')"
[[ -n "$IMG" ]] || { echo "❌ Could not read image from revision $REV"; exit 1; }
echo "🖼️ Live revision image: $IMG"

# -------- HEALTH CHECKS --------
if command -v curl >/dev/null; then
  echo "→ GET /"
  if command -v jq >/dev/null; then curl -sS "$BASE_URL/" | jq . || true; else curl -sS "$BASE_URL/" || true; fi

  echo "→ GET /healthz (fallback to /health)"
  if ! curl -sS -f "$BASE_URL/healthz" >/dev/null; then
    curl -sS -f "$BASE_URL/health" || echo "health endpoint non-200 (check logs)"
  fi

  echo "→ GET /snapshot"
  if command -v jq >/dev/null; then curl -sS "$BASE_URL/snapshot" | jq . || true; else curl -sS "$BASE_URL/snapshot" || true; fi
else
  echo "curl not installed; skipping health checks."
fi

# -------- TAG THE LIVE DIGEST WITH TIMESTAMP & LATEST (best-effort) --------
# If IMG is ...@sha256:..., extract digest; if it's already a tagged reference, skip tagging.
if [[ "$IMG" == *@sha256:* ]]; then
  DIGEST="${IMG##*@}"
  DEST_BASE="asia-south2-docker.pkg.dev/${PROJECT_ID}/${AR_REPO}/${IMAGE}"
  echo "🏷️  Tagging digest $DIGEST as :$TAG and :latest (best-effort)"
  gcloud auth configure-docker asia-south2-docker.pkg.dev -q || true
  gcloud artifacts docker tags add "${DEST_BASE}@${DIGEST}" "${DEST_BASE}:${TAG}" || true
  gcloud artifacts docker tags add "${DEST_BASE}@${DIGEST}" "${DEST_BASE}:latest" || true
  echo "✅ Tag attempts finished"
else
  echo "ℹ️ Image already referenced by tag (${IMG##*:}); skipping digest tagging."
fi

# -------- RECENT LOGS --------
echo "🪵 Recent logs:"
gcloud run services logs read "$SERVICE" --region="$REGION" --limit=50 || true

echo "🎉 Done."

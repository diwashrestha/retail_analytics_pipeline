#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${PROJECT_ROOT}"

PROFILE="${DATABRICKS_PROFILE:-einkaufpark-free}"
TARGET="dev"

echo
echo "========================================="
echo " Einkaufpark development deployment"
echo "========================================="
echo

echo "[1/4] Running local unit tests..."
python -m pytest tests/unit -q

echo
echo "[2/4] Validating Databricks bundle..."
databricks bundle validate \
  -t "$TARGET" \
  --profile "$PROFILE"

echo
echo "[3/4] Deploying Databricks bundle..."
databricks bundle deploy \
  -t "$TARGET" \
  --profile "$PROFILE"

echo
echo "[4/4] Running medallion workflow..."
databricks bundle run \
  -t "$TARGET" \
  --profile "$PROFILE" \
  retail_medallion_job

echo
echo "========================================="
echo " Deployment and validation completed"
echo "========================================="
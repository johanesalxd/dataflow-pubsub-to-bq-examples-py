#!/bin/bash

# Cleanup Script for Schema-Driven Pipeline
#
# Tears down all resources created by run_dataflow_schema_driven.sh and
# scripts/run_schema_evolution.sh so you can start from scratch.
#
# Resources deleted:
#   1. Dataflow job (cancelled if running)
#   2. Pub/Sub subscriptions (pipeline, v1 source, v2 source)
#   3. Pub/Sub schema topic
#   4. Pub/Sub schema (and all revisions)
#   5. BigQuery table
#
# Resources NOT deleted (shared across all 3 pipelines):
#   - GCS bucket (gs://${PROJECT_ID}-dataflow-bucket)
#   - BigQuery dataset (demo_dataset)
#
# Usage:
#   ./scripts/cleanup_schema_driven.sh           # interactive (confirms before deleting)
#   ./scripts/cleanup_schema_driven.sh --force    # no confirmation prompt

set -e

# --- Configuration (must match run_dataflow_schema_driven.sh) ---
PROJECT_ID="your-project-id"
REGION="us-central1"
BIGQUERY_DATASET="demo_dataset"
BIGQUERY_TABLE="taxi_events_schema"
SCHEMA_NAME="taxi-ride-schema"
SCHEMA_TOPIC="taxi_telemetry_schema"
SCHEMA_SUBSCRIPTION="taxi_telemetry_schema_sub"
SOURCE_SUBSCRIPTION="taxi_telemetry_schema_source"
V2_SOURCE_SUBSCRIPTION="taxi_telemetry_schema_v2_source"

# Full resource paths
FULL_TABLE="${PROJECT_ID}:${BIGQUERY_DATASET}.${BIGQUERY_TABLE}"

# --- Parse flags ---
FORCE=false
if [[ "$1" == "--force" ]]; then
    FORCE=true
fi

# --- Confirmation ---
echo "=== Schema-Driven Pipeline Cleanup ==="
echo ""
echo "This will delete the following resources:"
echo "  Dataflow job:    dataflow-schema-driven-* (cancel if running)"
echo "  Subscriptions:   ${SCHEMA_SUBSCRIPTION}"
echo "                   ${SOURCE_SUBSCRIPTION}"
echo "                   ${V2_SOURCE_SUBSCRIPTION}"
echo "  Topic:           ${SCHEMA_TOPIC}"
echo "  Schema:          ${SCHEMA_NAME} (and all revisions)"
echo "  BigQuery table:  ${FULL_TABLE}"
echo ""

if [[ "${FORCE}" != true ]]; then
    read -p "Are you sure? (y/N) " -n 1 -r
    echo ""
    if [[ ! "${REPLY}" =~ ^[Yy]$ ]]; then
        echo "Cancelled."
        exit 0
    fi
    echo ""
fi

# --- Helper function ---
delete_if_exists() {
    local resource_type="$1"
    local resource_name="$2"
    local check_cmd="$3"
    local delete_cmd="$4"

    echo -n "  ${resource_type}: ${resource_name} ... "
    if eval "${check_cmd}" 2>/dev/null; then
        eval "${delete_cmd}" 2>/dev/null
        echo "deleted."
    else
        echo "not found (skipping)."
    fi
}

# ===================================================================
# Step 1: Cancel running Dataflow jobs
# ===================================================================
echo "--- Step 1: Cancel running Dataflow jobs ---"
CURRENT_JOB_ID=$(gcloud dataflow jobs list \
    --region="${REGION}" \
    --project="${PROJECT_ID}" \
    --filter="name~dataflow-schema-driven AND state=Running" \
    --format="value(id)" \
    --limit=1 2>/dev/null || true)

if [[ -n "${CURRENT_JOB_ID}" ]]; then
    echo "  Found running job: ${CURRENT_JOB_ID}"
    echo "  Cancelling..."
    gcloud dataflow jobs cancel "${CURRENT_JOB_ID}" \
        --region="${REGION}" \
        --project="${PROJECT_ID}"
    echo "  Waiting for cancellation to complete..."
    while true; do
        STATE=$(gcloud dataflow jobs describe "${CURRENT_JOB_ID}" \
            --region="${REGION}" \
            --project="${PROJECT_ID}" \
            --format="value(currentState)" 2>/dev/null || true)
        if [[ "${STATE}" != "JOB_STATE_RUNNING" && "${STATE}" != "JOB_STATE_CANCELLING" ]]; then
            echo "  Job cancelled (state: ${STATE})."
            break
        fi
        echo "    Still cancelling (state: ${STATE})..."
        sleep 5
    done
else
    echo "  No running schema-driven jobs found."
fi
echo ""

# ===================================================================
# Step 2: Delete Pub/Sub subscriptions
# ===================================================================
echo "--- Step 2: Delete Pub/Sub subscriptions ---"

delete_if_exists "Subscription" "${SCHEMA_SUBSCRIPTION}" \
    "gcloud pubsub subscriptions describe ${SCHEMA_SUBSCRIPTION} --project=${PROJECT_ID}" \
    "gcloud pubsub subscriptions delete ${SCHEMA_SUBSCRIPTION} --project=${PROJECT_ID} --quiet"

delete_if_exists "Subscription" "${SOURCE_SUBSCRIPTION}" \
    "gcloud pubsub subscriptions describe ${SOURCE_SUBSCRIPTION} --project=${PROJECT_ID}" \
    "gcloud pubsub subscriptions delete ${SOURCE_SUBSCRIPTION} --project=${PROJECT_ID} --quiet"

delete_if_exists "Subscription" "${V2_SOURCE_SUBSCRIPTION}" \
    "gcloud pubsub subscriptions describe ${V2_SOURCE_SUBSCRIPTION} --project=${PROJECT_ID}" \
    "gcloud pubsub subscriptions delete ${V2_SOURCE_SUBSCRIPTION} --project=${PROJECT_ID} --quiet"

echo ""

# ===================================================================
# Step 3: Delete schema topic
# ===================================================================
echo "--- Step 3: Delete schema topic ---"

delete_if_exists "Topic" "${SCHEMA_TOPIC}" \
    "gcloud pubsub topics describe ${SCHEMA_TOPIC} --project=${PROJECT_ID}" \
    "gcloud pubsub topics delete ${SCHEMA_TOPIC} --project=${PROJECT_ID} --quiet"

echo ""

# ===================================================================
# Step 4: Delete Pub/Sub schema (and all revisions)
# ===================================================================
echo "--- Step 4: Delete Pub/Sub schema ---"

delete_if_exists "Schema" "${SCHEMA_NAME}" \
    "gcloud pubsub schemas describe ${SCHEMA_NAME} --project=${PROJECT_ID}" \
    "gcloud pubsub schemas delete ${SCHEMA_NAME} --project=${PROJECT_ID} --quiet"

echo ""

# ===================================================================
# Step 5: Delete BigQuery table
# ===================================================================
echo "--- Step 5: Delete BigQuery table ---"

echo -n "  Table: ${FULL_TABLE} ... "
if bq show "${FULL_TABLE}" 2>/dev/null; then
    bq rm -f -t "${FULL_TABLE}"
    echo "deleted."
else
    echo "not found (skipping)."
fi

echo ""
echo "=== Cleanup Complete ==="
echo ""
echo "To start fresh, run:"
echo "  ./run_dataflow_schema_driven.sh"
echo ""

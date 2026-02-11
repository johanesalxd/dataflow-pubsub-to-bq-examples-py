#!/bin/bash

# Cleanup Script for BQ Throughput Ceiling Test
#
# Tears down all resources created by run_perf_test.sh so you can start
# from scratch or run a new test round with different configuration.
#
# Resources deleted:
#   1. Dataflow jobs (cancelled if running)
#   2. Pub/Sub subscriptions (sub_a, sub_b)
#   3. Pub/Sub topics (topic_a, topic_b)
#   4. BigQuery tables (taxi_events_perf, taxi_events_perf_dlq)
#
# Resources NOT deleted (shared across pipelines):
#   - GCS bucket
#   - BigQuery dataset (demo_dataset_asia)
#
# Usage:
#   ./scripts/cleanup_perf_test.sh           # interactive
#   ./scripts/cleanup_perf_test.sh --force   # no confirmation

set -e

# --- Configuration (must match run_perf_test.sh) ---
PROJECT_ID="your-project-id"
REGION="asia-southeast1"
BIGQUERY_DATASET="demo_dataset_asia"
BIGQUERY_TABLE="taxi_events_perf"
TOPIC_A="perf_test_topic_a"
TOPIC_B="perf_test_topic_b"
SUB_A="perf_test_sub_a"
SUB_B="perf_test_sub_b"

# Full resource paths
FULL_TABLE="${PROJECT_ID}:${BIGQUERY_DATASET}.${BIGQUERY_TABLE}"

# --- Parse flags ---
FORCE=false
if [[ "$1" == "--force" ]]; then
    FORCE=true
fi

# --- Confirmation ---
echo "=== BQ Throughput Test Cleanup ==="
echo ""
echo "This will delete the following resources:"
echo "  Dataflow jobs:   dataflow-perf-test-* (cancel if running)"
echo "  Subscriptions:   ${SUB_A}, ${SUB_B}"
echo "  Topics:          ${TOPIC_A}, ${TOPIC_B}"
echo "  BigQuery tables: ${FULL_TABLE}"
echo "                   ${FULL_TABLE}_dlq"
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

JOB_IDS=$(gcloud dataflow jobs list \
    --region="${REGION}" \
    --project="${PROJECT_ID}" \
    --filter="name~dataflow-perf-test AND state=Running" \
    --format="value(id)" 2>/dev/null || true)

if [[ -n "${JOB_IDS}" ]]; then
    for JOB_ID in ${JOB_IDS}; do
        echo "  Cancelling job: ${JOB_ID}"
        gcloud dataflow jobs cancel "${JOB_ID}" \
            --region="${REGION}" \
            --project="${PROJECT_ID}" 2>/dev/null || true
    done
    echo "  Waiting for cancellation..."
    sleep 10
else
    echo "  No running perf-test jobs found."
fi
echo ""

# ===================================================================
# Step 2: Delete Pub/Sub subscriptions
# ===================================================================
echo "--- Step 2: Delete Pub/Sub subscriptions ---"

delete_if_exists "Subscription" "${SUB_A}" \
    "gcloud pubsub subscriptions describe ${SUB_A} --project=${PROJECT_ID}" \
    "gcloud pubsub subscriptions delete ${SUB_A} --project=${PROJECT_ID} --quiet"

delete_if_exists "Subscription" "${SUB_B}" \
    "gcloud pubsub subscriptions describe ${SUB_B} --project=${PROJECT_ID}" \
    "gcloud pubsub subscriptions delete ${SUB_B} --project=${PROJECT_ID} --quiet"

echo ""

# ===================================================================
# Step 3: Delete Pub/Sub topics
# ===================================================================
echo "--- Step 3: Delete Pub/Sub topics ---"

delete_if_exists "Topic" "${TOPIC_A}" \
    "gcloud pubsub topics describe ${TOPIC_A} --project=${PROJECT_ID}" \
    "gcloud pubsub topics delete ${TOPIC_A} --project=${PROJECT_ID} --quiet"

delete_if_exists "Topic" "${TOPIC_B}" \
    "gcloud pubsub topics describe ${TOPIC_B} --project=${PROJECT_ID}" \
    "gcloud pubsub topics delete ${TOPIC_B} --project=${PROJECT_ID} --quiet"

echo ""

# ===================================================================
# Step 4: Delete BigQuery tables
# ===================================================================
echo "--- Step 4: Delete BigQuery tables ---"

echo -n "  Table: ${FULL_TABLE} ... "
if bq show "${FULL_TABLE}" 2>/dev/null; then
    bq rm -f -t "${FULL_TABLE}"
    echo "deleted."
else
    echo "not found (skipping)."
fi

echo -n "  Table: ${FULL_TABLE}_dlq ... "
if bq show "${FULL_TABLE}_dlq" 2>/dev/null; then
    bq rm -f -t "${FULL_TABLE}_dlq"
    echo "deleted."
else
    echo "not found (skipping)."
fi

echo ""
echo "=== Cleanup Complete ==="
echo ""
echo "To run a new test:"
echo "  ./scripts/run_perf_test.sh setup"
echo ""

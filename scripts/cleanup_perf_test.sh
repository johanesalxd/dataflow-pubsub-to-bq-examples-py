#!/bin/bash

# Cleanup Script for BQ Throughput Ceiling Test
#
# Tears down all resources created by run_perf_test.sh so you can start
# from scratch or run a new test round with different configuration.
#
# Resources deleted:
#   1. Dataflow jobs (cancelled if running)
#   2. Pub/Sub subscriptions (perf_test_sub_a, perf_test_sub_b, ...)
#   3. Pub/Sub topic (perf_test_topic)
#   4. BigQuery tables:
#      - JSON pipeline: taxi_events_perf, taxi_events_perf_dlq
#      - Avro pipeline:  taxi_events_perf_enroute, _pickup, _dropoff, _waiting
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
TOPIC="perf_test_topic"

# Consumer labels to clean up (covers a-h)
LABELS=(a b c d e f g h)

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
echo "  Dataflow jobs:   dataflow-perf-* (test, publisher, avro)"
echo "  Subscriptions:   perf_test_sub_{a..h} (if they exist)"
echo "  Topic:           ${TOPIC}"
echo "  BigQuery tables: ${FULL_TABLE}"
echo "                   ${FULL_TABLE}_dlq"
echo "                   ${FULL_TABLE}_enroute, _pickup, _dropoff, _waiting"
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
    --filter="name~dataflow-perf AND state=Running" \
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

for LABEL in "${LABELS[@]}"; do
    SUB_NAME="perf_test_sub_${LABEL}"
    delete_if_exists "Subscription" "${SUB_NAME}" \
        "gcloud pubsub subscriptions describe ${SUB_NAME} --project=${PROJECT_ID}" \
        "gcloud pubsub subscriptions delete ${SUB_NAME} --project=${PROJECT_ID} --quiet"
done

echo ""

# ===================================================================
# Step 3: Delete Pub/Sub topic
# ===================================================================
echo "--- Step 3: Delete Pub/Sub topic ---"

delete_if_exists "Topic" "${TOPIC}" \
    "gcloud pubsub topics describe ${TOPIC} --project=${PROJECT_ID}" \
    "gcloud pubsub topics delete ${TOPIC} --project=${PROJECT_ID} --quiet"

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

# Avro pipeline typed tables (round 5)
for STATUS in enroute pickup dropoff waiting; do
    AVRO_TABLE="${FULL_TABLE}_${STATUS}"
    echo -n "  Table: ${AVRO_TABLE} ... "
    if bq show "${AVRO_TABLE}" 2>/dev/null; then
        bq rm -f -t "${AVRO_TABLE}"
        echo "deleted."
    else
        echo "not found (skipping)."
    fi
done

echo ""
echo "=== Cleanup Complete ==="
echo ""
echo "To run a new test:"
echo "  ./scripts/run_perf_test.sh setup"
echo ""

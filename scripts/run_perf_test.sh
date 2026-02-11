#!/bin/bash

# BQ Throughput Ceiling Test -- Orchestration Script
#
# Proves the "Noisy Neighbor" / "Shared Pipe" ceiling by launching two
# identical Dataflow jobs that write to the same BigQuery table and
# observing throughput degradation.
#
# Architecture (per job):
#   Publisher A (10k msgs/sec, ~10KB each)
#       -> Topic A -> Sub A -> Dataflow Job A (pipeline_json.py)
#                                   -> BQ: taxi_events_perf (JSON column)
#   Publisher B (same config)
#       -> Topic B -> Sub B -> Dataflow Job B (same BQ table)
#
# Usage:
#   ./scripts/run_perf_test.sh setup         # create GCP resources
#   ./scripts/run_perf_test.sh publisher-a   # start publisher A (foreground)
#   ./scripts/run_perf_test.sh publisher-b   # start publisher B (foreground)
#   ./scripts/run_perf_test.sh job-a         # launch Dataflow Job A
#   ./scripts/run_perf_test.sh job-b         # launch Dataflow Job B
#   ./scripts/run_perf_test.sh monitor       # print monitoring URLs
#   ./scripts/run_perf_test.sh dry-run       # validate message sizes (no GCP)

set -e

# ===================================================================
# Configuration -- adjust these for each test round
# ===================================================================
PROJECT_ID="your-project-id"
REGION="asia-southeast1"
TEMP_BUCKET="gs://${PROJECT_ID}-dataflow-bucket"
BIGQUERY_DATASET="demo_dataset_asia"
BIGQUERY_TABLE="taxi_events_perf"

# Worker configuration (scale: 3, 10, 20)
NUM_WORKERS=3
MACHINE_TYPE="n2-standard-4"

# Publisher configuration
TARGET_MBPS=100
MESSAGE_SIZE_BYTES=10000
DURATION_MINUTES=30

# Topic/subscription names
TOPIC_A="perf_test_topic_a"
TOPIC_B="perf_test_topic_b"
SUB_A="perf_test_sub_a"
SUB_B="perf_test_sub_b"

# Derived values
FULL_TABLE="${PROJECT_ID}:${BIGQUERY_DATASET}.${BIGQUERY_TABLE}"
FULL_TOPIC_A="projects/${PROJECT_ID}/topics/${TOPIC_A}"
FULL_TOPIC_B="projects/${PROJECT_ID}/topics/${TOPIC_B}"
FULL_SUB_A="projects/${PROJECT_ID}/subscriptions/${SUB_A}"
FULL_SUB_B="projects/${PROJECT_ID}/subscriptions/${SUB_B}"
JOB_NAME_A="dataflow-perf-test-a-$(date +%Y%m%d-%H%M%S)"
JOB_NAME_B="dataflow-perf-test-b-$(date +%Y%m%d-%H%M%S)"

# ===================================================================
# Functions
# ===================================================================

print_config() {
    echo "=== BQ Throughput Ceiling Test ==="
    echo ""
    echo "Configuration:"
    echo "  Project:          ${PROJECT_ID}"
    echo "  Region:           ${REGION}"
    echo "  BQ Table:         ${FULL_TABLE}"
    echo "  Workers per job:  ${NUM_WORKERS}x ${MACHINE_TYPE}"
    echo "  Publisher rate:   ${TARGET_MBPS} MB/s per topic"
    echo "  Message size:     ${MESSAGE_SIZE_BYTES} bytes"
    echo "  Duration:         ${DURATION_MINUTES} minutes"
    echo ""
    echo "  BQ Storage Write API quota (${REGION}):"
    echo "    Throughput:     300 MB/s (regional)"
    echo "    Connections:    1,000 concurrent"
    echo ""
}

do_setup() {
    print_config
    echo "--- Setting up GCP resources ---"
    echo ""

    # 1. GCS bucket
    echo "1. Checking GCS bucket..."
    if ! gsutil ls "${TEMP_BUCKET}" 2>/dev/null; then
        echo "   Creating GCS bucket..."
        gsutil mb -p "${PROJECT_ID}" -l "${REGION}" "${TEMP_BUCKET}"
    else
        echo "   GCS bucket already exists."
    fi

    # 2. BigQuery dataset
    echo "2. Checking BigQuery dataset..."
    if ! bq show --dataset "${PROJECT_ID}:${BIGQUERY_DATASET}" 2>/dev/null; then
        echo "   Creating BigQuery dataset..."
        bq mk --dataset --location="${REGION}" "${PROJECT_ID}:${BIGQUERY_DATASET}"
    else
        echo "   BigQuery dataset already exists."
    fi

    # 3. BigQuery table (JSON column schema)
    echo "3. Checking BigQuery table..."
    if ! bq show "${FULL_TABLE}" 2>/dev/null; then
        echo "   Creating BigQuery table with JSON schema..."
        bq mk \
            --table \
            --time_partitioning_field=publish_time \
            --time_partitioning_type=DAY \
            --clustering_fields=publish_time \
            --schema=subscription_name:STRING,message_id:STRING,publish_time:TIMESTAMP,processing_time:TIMESTAMP,attributes:STRING,payload:JSON \
            "${FULL_TABLE}"
    else
        echo "   BigQuery table already exists."
    fi

    # 4. BigQuery DLQ table
    echo "4. Checking BigQuery DLQ table..."
    if ! bq show "${FULL_TABLE}_dlq" 2>/dev/null; then
        echo "   Creating BigQuery DLQ table..."
        bq mk \
            --table \
            --time_partitioning_field=processing_time \
            --time_partitioning_type=DAY \
            --schema=processing_time:TIMESTAMP,error_message:STRING,stack_trace:STRING,original_payload:STRING,subscription_name:STRING \
            "${FULL_TABLE}_dlq"
    else
        echo "   BigQuery DLQ table already exists."
    fi

    # 5. Pub/Sub topics
    echo "5. Checking Pub/Sub topics..."
    for TOPIC in "${TOPIC_A}" "${TOPIC_B}"; do
        if ! gcloud pubsub topics describe "${TOPIC}" --project="${PROJECT_ID}" 2>/dev/null; then
            echo "   Creating topic: ${TOPIC}"
            gcloud pubsub topics create "${TOPIC}" --project="${PROJECT_ID}"
        else
            echo "   Topic ${TOPIC} already exists."
        fi
    done

    # 6. Pub/Sub subscriptions
    echo "6. Checking Pub/Sub subscriptions..."
    create_sub_if_needed() {
        local SUB_NAME="$1"
        local TOPIC_PATH="$2"
        if ! gcloud pubsub subscriptions describe "${SUB_NAME}" --project="${PROJECT_ID}" 2>/dev/null; then
            echo "   Creating subscription: ${SUB_NAME}"
            gcloud pubsub subscriptions create "${SUB_NAME}" \
                --project="${PROJECT_ID}" \
                --topic="${TOPIC_PATH}"
        else
            echo "   Subscription ${SUB_NAME} already exists."
        fi
    }
    create_sub_if_needed "${SUB_A}" "${FULL_TOPIC_A}"
    create_sub_if_needed "${SUB_B}" "${FULL_TOPIC_B}"

    # 7. Build wheel
    echo "7. Building package wheel..."
    uv sync
    uv build --wheel
    WHEEL_FILE=$(ls -t dist/*.whl | head -1)
    echo "   Built: ${WHEEL_FILE}"

    echo ""
    echo "=== Setup Complete ==="
    echo ""
    echo "Next steps:"
    echo "  1. Start publisher:  ./scripts/run_perf_test.sh publisher-a"
    echo "  2. Launch job:       ./scripts/run_perf_test.sh job-a"
    echo "  3. Wait 15 min for steady state"
    echo "  4. Start publisher:  ./scripts/run_perf_test.sh publisher-b"
    echo "  5. Launch job:       ./scripts/run_perf_test.sh job-b"
    echo "  6. Monitor:          ./scripts/run_perf_test.sh monitor"
    echo ""
}

do_publisher() {
    local LABEL="$1"
    local TOPIC_PATH="$2"

    echo "=== Starting Publisher ${LABEL} ==="
    echo "  Topic:    ${TOPIC_PATH}"
    echo "  Rate:     ${TARGET_MBPS} MB/s"
    echo "  Size:     ${MESSAGE_SIZE_BYTES} bytes/msg"
    echo "  Duration: ${DURATION_MINUTES} minutes"
    echo ""
    echo "Press Ctrl+C to stop."
    echo ""

    uv run python scripts/synthetic_publisher.py \
        --project="${PROJECT_ID}" \
        --topic="${TOPIC_PATH}" \
        --target_mbps="${TARGET_MBPS}" \
        --message_size_bytes="${MESSAGE_SIZE_BYTES}" \
        --duration_minutes="${DURATION_MINUTES}"
}

do_job() {
    local LABEL="$1"
    local JOB_NAME="$2"
    local FULL_SUB="$3"
    local SUB_NAME="$4"

    WHEEL_FILE=$(ls -t dist/*.whl | head -1)
    if [[ -z "${WHEEL_FILE}" ]]; then
        echo "ERROR: No wheel file found. Run './scripts/run_perf_test.sh setup' first."
        exit 1
    fi

    echo "=== Launching Dataflow Job ${LABEL} ==="
    echo "  Job name:     ${JOB_NAME}"
    echo "  Subscription: ${FULL_SUB}"
    echo "  Workers:      ${NUM_WORKERS}x ${MACHINE_TYPE}"
    echo "  BQ table:     ${FULL_TABLE}"
    echo ""

    uv run python -m dataflow_pubsub_to_bq.pipeline_json \
        --runner=DataflowRunner \
        --project="${PROJECT_ID}" \
        --region="${REGION}" \
        --job_name="${JOB_NAME}" \
        --temp_location="${TEMP_BUCKET}/temp" \
        --staging_location="${TEMP_BUCKET}/staging" \
        --subscription="${FULL_SUB}" \
        --output_table="${FULL_TABLE}" \
        --subscription_name="${SUB_NAME}" \
        --streaming \
        --experiments=use_runner_v2 \
        --sdk_container_image=apache/beam_python3.12_sdk:2.70.0 \
        --extra_packages="${WHEEL_FILE}" \
        --machine_type="${MACHINE_TYPE}" \
        --num_workers="${NUM_WORKERS}" \
        --max_num_workers="${NUM_WORKERS}" \
        --enable_streaming_engine

    echo ""
    echo "=== Job ${LABEL} Submitted ==="
    echo ""
    echo "View job at:"
    echo "  https://console.cloud.google.com/dataflow/jobs/${REGION}/${JOB_NAME}?project=${PROJECT_ID}"
    echo ""
}

do_monitor() {
    print_config
    echo "--- Monitoring URLs ---"
    echo ""
    echo "Dataflow Jobs:"
    echo "  https://console.cloud.google.com/dataflow/jobs?project=${PROJECT_ID}&region=${REGION}"
    echo ""
    echo "BigQuery Table:"
    echo "  https://console.cloud.google.com/bigquery?project=${PROJECT_ID}&ws=!1m5!1m4!4m3!1s${PROJECT_ID}!2s${BIGQUERY_DATASET}!3s${BIGQUERY_TABLE}"
    echo ""
    echo "--- Key Metrics to Monitor (Cloud Monitoring) ---"
    echo ""
    echo "1. BQ Storage Write API throughput:"
    echo "   Metric: bigquerystorage.googleapis.com/write/append_bytes"
    echo "   Filter: resource.label.project_id = \"${PROJECT_ID}\""
    echo "   Quota:  AppendBytesThroughputPerProjectRegion"
    echo ""
    echo "2. BQ concurrent connections:"
    echo "   Metric: bigquerystorage.googleapis.com/write/max_active_streams"
    echo "   Quota:  ConcurrentWriteConnectionsPerProjectRegion (limit: 1,000)"
    echo ""
    echo "3. Pub/Sub subscription backlog:"
    echo "   Metric: pubsub.googleapis.com/subscription/num_undelivered_messages"
    echo "   Subscriptions: ${SUB_A}, ${SUB_B}"
    echo ""
    echo "4. Dataflow worker CPU:"
    echo "   View in Dataflow job monitoring tab > Worker CPU"
    echo ""
    echo "5. BQ write wall time:"
    echo "   View in Dataflow job > Execution Details > WriteToBigQuery stage"
    echo ""
    echo "--- What to Look For ---"
    echo ""
    echo "Phase 1 (single job):"
    echo "  - CPU < 30% = I/O bound (expected)"
    echo "  - Pub/Sub backlog stable = pipeline keeping up"
    echo "  - BQ write throughput ~ 100 MB/s"
    echo ""
    echo "Phase 2 (two jobs, same table):"
    echo "  - Job A throughput drops after Job B starts = contention"
    echo "  - Combined < 200 MB/s = shared resource limit"
    echo "  - Check BQ quota usage: if < 100% = not quota-bound"
    echo "  - Check connections: if near 1,000 = connection-bound"
    echo ""
    echo "--- IAM Quota Check ---"
    echo ""
    echo "  https://console.cloud.google.com/iam-admin/quotas?project=${PROJECT_ID}&metric=bigquerystorage.googleapis.com"
    echo ""
}

do_dry_run() {
    echo "=== Dry Run -- Validating Message Sizes ==="
    echo ""
    uv run python scripts/synthetic_publisher.py \
        --project="${PROJECT_ID}" \
        --topic="projects/${PROJECT_ID}/topics/dry-run" \
        --target_mbps="${TARGET_MBPS}" \
        --message_size_bytes="${MESSAGE_SIZE_BYTES}" \
        --dry-run
}

# ===================================================================
# Main dispatch
# ===================================================================

COMMAND="${1:-help}"

case "${COMMAND}" in
    setup)
        do_setup
        ;;
    publisher-a)
        do_publisher "A" "${FULL_TOPIC_A}"
        ;;
    publisher-b)
        do_publisher "B" "${FULL_TOPIC_B}"
        ;;
    job-a)
        do_job "A" "${JOB_NAME_A}" "${FULL_SUB_A}" "${SUB_A}"
        ;;
    job-b)
        do_job "B" "${JOB_NAME_B}" "${FULL_SUB_B}" "${SUB_B}"
        ;;
    monitor)
        do_monitor
        ;;
    dry-run)
        do_dry_run
        ;;
    *)
        echo "Usage: $0 {setup|publisher-a|publisher-b|job-a|job-b|monitor|dry-run}"
        echo ""
        echo "Commands:"
        echo "  setup        Create all GCP resources (topics, subs, BQ table)"
        echo "  publisher-a  Start synthetic publisher for Topic A (foreground)"
        echo "  publisher-b  Start synthetic publisher for Topic B (foreground)"
        echo "  job-a        Launch Dataflow Job A (reads Sub A, writes to BQ)"
        echo "  job-b        Launch Dataflow Job B (reads Sub B, same BQ table)"
        echo "  monitor      Print monitoring URLs and metric names"
        echo "  dry-run      Validate message sizes without publishing"
        echo ""
        echo "Typical workflow:"
        echo "  1. ./scripts/run_perf_test.sh setup"
        echo "  2. ./scripts/run_perf_test.sh publisher-a   # terminal 1"
        echo "  3. ./scripts/run_perf_test.sh job-a          # terminal 2"
        echo "  4. Wait 15 min for steady state"
        echo "  5. ./scripts/run_perf_test.sh publisher-b   # terminal 3"
        echo "  6. ./scripts/run_perf_test.sh job-b          # terminal 4"
        echo "  7. ./scripts/run_perf_test.sh monitor"
        exit 1
        ;;
esac

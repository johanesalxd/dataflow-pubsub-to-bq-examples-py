#!/bin/bash

# BQ Throughput Ceiling Test -- Orchestration Script
#
# Proves the "Noisy Neighbor" / "Shared Pipe" ceiling by launching N
# identical Dataflow jobs that write to the same BigQuery table and
# observing throughput degradation.
#
# Architecture:
#   Publisher (Dataflow batch job, auto-terminates)
#       -> Topic (perf_test_topic)
#           -> Sub A (perf_test_sub_a) -> Dataflow Job A (pipeline_json.py)
#           -> Sub B (perf_test_sub_b) -> Dataflow Job B (same BQ table)
#           -> Sub N ...               -> Dataflow Job N (same BQ table)
#
# Usage (JSON pipeline -- rounds 1-4):
#   ./scripts/run_perf_test.sh setup            # create GCP resources
#   ./scripts/run_perf_test.sh publish           # launch JSON publisher
#   ./scripts/run_perf_test.sh publish-status    # check publisher job status
#   ./scripts/run_perf_test.sh job a             # launch Python consumer Job A
#   ./scripts/run_perf_test.sh java-setup        # build Java pipeline JAR
#   ./scripts/run_perf_test.sh java-job a        # launch Java JSON consumer Job A
#   ./scripts/run_perf_test.sh dry-run           # validate JSON message sizes
#
# Usage (Avro pipeline -- round 5):
#   ./scripts/run_perf_test.sh setup-avro        # create typed BQ tables + build all
#   ./scripts/run_perf_test.sh publish-avro      # launch Avro publisher
#   ./scripts/run_perf_test.sh java-avro-job a   # launch Java Avro consumer Job A
#   ./scripts/run_perf_test.sh dry-run-avro      # validate Avro sizes + expansion ratio
#   ./scripts/run_perf_test.sh monitor           # print monitoring URLs

set -e

# ===================================================================
# Configuration -- adjust these for each test round
# ===================================================================
PROJECT_ID="your-project-id"
REGION="asia-southeast1"
TEMP_BUCKET="gs://${PROJECT_ID}-dataflow-bucket"
BIGQUERY_DATASET="demo_dataset_asia"
BIGQUERY_TABLE="taxi_events_perf"

# Number of consumer jobs (subscriptions a, b, c, ...)
NUM_CONSUMERS=2

# Worker configuration
CONSUMER_NUM_WORKERS=3
CONSUMER_MACHINE_TYPE="n2-standard-4"
PUBLISHER_NUM_WORKERS=5
PUBLISHER_MACHINE_TYPE="n2-standard-4"

# Java consumer configuration (matches production: 3x n2-highcpu-8 = 24 vCPUs)
JAVA_CONSUMER_NUM_WORKERS=3
JAVA_CONSUMER_MACHINE_TYPE="n2-highcpu-8"
JAR_FILE="java/target/dataflow-pubsub-to-bq-json-1.0-SNAPSHOT.jar"

# Message configuration
NUM_MESSAGES=400000000   # 400M msgs = ~35 GB Avro
MESSAGE_SIZE_BYTES=500

# BigQuery write semantics: set to true for at-least-once, false for exactly-once
USE_AT_LEAST_ONCE=true

# Topic/subscription names
TOPIC="perf_test_topic"
FULL_TOPIC="projects/${PROJECT_ID}/topics/${TOPIC}"

# Consumer labels (a-h, extend if needed)
LABELS=(a b c d e f g h)

# Derived values
FULL_TABLE="${PROJECT_ID}:${BIGQUERY_DATASET}.${BIGQUERY_TABLE}"

# ===================================================================
# Functions
# ===================================================================

print_config() {
    echo "=== BQ Throughput Ceiling Test ==="
    echo ""
    echo "Configuration:"
    echo "  Project:            ${PROJECT_ID}"
    echo "  Region:             ${REGION}"
    echo "  BQ Table:           ${FULL_TABLE}"
    echo "  Topic:              ${FULL_TOPIC}"
    echo "  Consumers:          ${NUM_CONSUMERS}"
    echo "  Consumer workers:   ${CONSUMER_NUM_WORKERS}x ${CONSUMER_MACHINE_TYPE} (Python)"
    echo "  Java consumers:    ${JAVA_CONSUMER_NUM_WORKERS}x ${JAVA_CONSUMER_MACHINE_TYPE} (Java)"
    echo "  Publisher workers:  ${PUBLISHER_NUM_WORKERS}x ${PUBLISHER_MACHINE_TYPE}"
    echo "  Messages:           ${NUM_MESSAGES}"
    echo "  Message size:       ${MESSAGE_SIZE_BYTES} bytes"
    echo "  At-least-once:      ${USE_AT_LEAST_ONCE}"
    echo ""
    local TOTAL_GB=$(( NUM_MESSAGES * MESSAGE_SIZE_BYTES / 1000000000 ))
    echo "  Total data:         ~${TOTAL_GB} GB"
    echo ""
    echo "  BQ Storage Write API quota (${REGION}):"
    echo "    Throughput:       300 MB/s (regional)"
    echo "    Connections:      1,000 concurrent"
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

    # 5. Pub/Sub topic (single topic)
    echo "5. Checking Pub/Sub topic..."
    if ! gcloud pubsub topics describe "${TOPIC}" --project="${PROJECT_ID}" 2>/dev/null; then
        echo "   Creating topic: ${TOPIC}"
        gcloud pubsub topics create "${TOPIC}" --project="${PROJECT_ID}"
    else
        echo "   Topic ${TOPIC} already exists."
    fi

    # 6. Pub/Sub subscriptions (one per consumer)
    echo "6. Checking Pub/Sub subscriptions (${NUM_CONSUMERS} consumers)..."
    for i in $(seq 0 $((NUM_CONSUMERS - 1))); do
        local LABEL="${LABELS[$i]}"
        local SUB_NAME="perf_test_sub_${LABEL}"
        if ! gcloud pubsub subscriptions describe "${SUB_NAME}" --project="${PROJECT_ID}" 2>/dev/null; then
            echo "   Creating subscription: ${SUB_NAME}"
            gcloud pubsub subscriptions create "${SUB_NAME}" \
                --project="${PROJECT_ID}" \
                --topic="${FULL_TOPIC}"
        else
            echo "   Subscription ${SUB_NAME} already exists."
        fi
    done

    # 7. Build wheel
    echo "7. Building package wheel..."
    uv sync
    uv build --wheel
    WHEEL_FILE=$(ls -t dist/*.whl | head -1)
    echo "   Built: ${WHEEL_FILE}"

    echo ""
    echo "=== Setup Complete ==="
    echo ""
    echo "Subscriptions created:"
    for i in $(seq 0 $((NUM_CONSUMERS - 1))); do
        echo "  perf_test_sub_${LABELS[$i]}"
    done
    echo ""
    echo "IMPORTANT: All subscriptions must exist BEFORE publishing."
    echo "If you need more consumers, increase NUM_CONSUMERS and re-run setup."
    echo ""
    echo "Next steps:"
    echo "  1. Publish:         ./scripts/run_perf_test.sh publish"
    echo "  2. Check status:    ./scripts/run_perf_test.sh publish-status"
    echo "  3. Launch Job A:    ./scripts/run_perf_test.sh job a"
    echo "  4. Wait 10 min for baseline"
    echo "  5. Launch Job B:    ./scripts/run_perf_test.sh job b"
    echo "  6. Monitor:         ./scripts/run_perf_test.sh monitor"
    echo ""
}

do_setup_avro() {
    print_config
    echo "--- Setting up GCP resources (Avro + Dynamic Routing) ---"
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

    # 3. BigQuery typed tables (one per ride_status)
    local AVRO_SCHEMA="subscription_name:STRING,message_id:STRING,publish_time:TIMESTAMP,processing_time:TIMESTAMP,ride_id:STRING,point_idx:INT64,latitude:FLOAT64,longitude:FLOAT64,timestamp:TIMESTAMP,meter_reading:FLOAT64,meter_increment:FLOAT64,ride_status:STRING,passenger_count:INT64,attributes:STRING"
    local STATUSES=(enroute pickup dropoff waiting)

    echo "3. Checking BigQuery typed tables (4 ride_status tables)..."
    for STATUS in "${STATUSES[@]}"; do
        local TABLE_NAME="${BIGQUERY_TABLE}_${STATUS}"
        local FULL_STATUS_TABLE="${PROJECT_ID}:${BIGQUERY_DATASET}.${TABLE_NAME}"
        if ! bq show "${FULL_STATUS_TABLE}" 2>/dev/null; then
            echo "   Creating table: ${TABLE_NAME}"
            bq mk \
                --table \
                --time_partitioning_field=publish_time \
                --time_partitioning_type=DAY \
                --clustering_fields=publish_time \
                --schema="${AVRO_SCHEMA}" \
                "${FULL_STATUS_TABLE}"
        else
            echo "   Table ${TABLE_NAME} already exists."
        fi
    done

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

    # 5. Pub/Sub topic (single topic)
    echo "5. Checking Pub/Sub topic..."
    if ! gcloud pubsub topics describe "${TOPIC}" --project="${PROJECT_ID}" 2>/dev/null; then
        echo "   Creating topic: ${TOPIC}"
        gcloud pubsub topics create "${TOPIC}" --project="${PROJECT_ID}"
    else
        echo "   Topic ${TOPIC} already exists."
    fi

    # 6. Pub/Sub subscriptions (one per consumer)
    echo "6. Checking Pub/Sub subscriptions (${NUM_CONSUMERS} consumers)..."
    for i in $(seq 0 $((NUM_CONSUMERS - 1))); do
        local LABEL="${LABELS[$i]}"
        local SUB_NAME="perf_test_sub_${LABEL}"
        if ! gcloud pubsub subscriptions describe "${SUB_NAME}" --project="${PROJECT_ID}" 2>/dev/null; then
            echo "   Creating subscription: ${SUB_NAME}"
            gcloud pubsub subscriptions create "${SUB_NAME}" \
                --project="${PROJECT_ID}" \
                --topic="${FULL_TOPIC}"
        else
            echo "   Subscription ${SUB_NAME} already exists."
        fi
    done

    # 7. Build wheel + Java JAR
    echo "7. Building Python wheel..."
    uv sync
    uv build --wheel
    WHEEL_FILE=$(ls -t dist/*.whl | head -1)
    echo "   Built: ${WHEEL_FILE}"

    echo "8. Building Java pipeline JAR..."
    mvn -f java/pom.xml clean package -DskipTests
    if [[ ! -f "${JAR_FILE}" ]]; then
        echo "   ERROR: JAR file not found at ${JAR_FILE}"
        exit 1
    fi
    echo "   Built: ${JAR_FILE}"

    echo ""
    echo "=== Avro Setup Complete ==="
    echo ""
    echo "Tables created:"
    for STATUS in "${STATUSES[@]}"; do
        echo "  ${BIGQUERY_TABLE}_${STATUS}"
    done
    echo "  ${BIGQUERY_TABLE}_dlq"
    echo ""
    echo "Next steps:"
    echo "  1. Publish Avro:    ./scripts/run_perf_test.sh publish-avro"
    echo "  2. Check status:    ./scripts/run_perf_test.sh publish-status"
    echo "  3. Launch Job A:    ./scripts/run_perf_test.sh java-avro-job a"
    echo "  4. Wait 10 min for baseline"
    echo "  5. Launch Job B:    ./scripts/run_perf_test.sh java-avro-job b"
    echo "  6. Monitor:         ./scripts/run_perf_test.sh monitor"
    echo ""
}

do_publish() {
    WHEEL_FILE=$(ls -t dist/*.whl | head -1)
    if [[ -z "${WHEEL_FILE}" ]]; then
        echo "ERROR: No wheel file found. Run './scripts/run_perf_test.sh setup' first."
        exit 1
    fi

    local JOB_NAME="dataflow-perf-publisher-$(date +%Y%m%d-%H%M%S)"
    local TOTAL_GB=$(( NUM_MESSAGES * MESSAGE_SIZE_BYTES / 1000000000 ))

    echo "=== Launching Dataflow Publisher ==="
    echo "  Job name:     ${JOB_NAME}"
    echo "  Topic:        ${FULL_TOPIC}"
    echo "  Messages:     ${NUM_MESSAGES}"
    echo "  Message size: ${MESSAGE_SIZE_BYTES} bytes"
    echo "  Total data:   ~${TOTAL_GB} GB"
    echo "  Workers:      ${PUBLISHER_NUM_WORKERS}x ${PUBLISHER_MACHINE_TYPE}"
    echo ""
    echo "  This is a BATCH job. It will auto-terminate after publishing."
    echo ""

    uv run python -m dataflow_pubsub_to_bq.pipeline_publisher \
        --runner=DataflowRunner \
        --project="${PROJECT_ID}" \
        --region="${REGION}" \
        --job_name="${JOB_NAME}" \
        --temp_location="${TEMP_BUCKET}/temp" \
        --staging_location="${TEMP_BUCKET}/staging" \
        --topic="${FULL_TOPIC}" \
        --num_messages="${NUM_MESSAGES}" \
        --message_size_bytes="${MESSAGE_SIZE_BYTES}" \
        --experiments=use_runner_v2 \
        --sdk_container_image=apache/beam_python3.12_sdk:2.70.0 \
        --extra_packages="${WHEEL_FILE}" \
        --machine_type="${PUBLISHER_MACHINE_TYPE}" \
        --num_workers="${PUBLISHER_NUM_WORKERS}" \
        --max_num_workers="${PUBLISHER_NUM_WORKERS}"

    echo ""
    echo "=== Publisher Job Submitted ==="
    echo ""
    echo "Check status:"
    echo "  ./scripts/run_perf_test.sh publish-status"
    echo ""
    echo "View job at:"
    echo "  https://console.cloud.google.com/dataflow/jobs/${REGION}?project=${PROJECT_ID}"
    echo ""
}

do_publish_avro() {
    WHEEL_FILE=$(ls -t dist/*.whl | head -1)
    if [[ -z "${WHEEL_FILE}" ]]; then
        echo "ERROR: No wheel file found. Run './scripts/run_perf_test.sh setup-avro' first."
        exit 1
    fi

    local JOB_NAME="dataflow-perf-avro-publisher-$(date +%Y%m%d-%H%M%S)"

    # Avro message size is ~85 bytes (schema-determined, no padding)
    local AVRO_MSG_SIZE=85
    local TOTAL_GB=$(( NUM_MESSAGES * AVRO_MSG_SIZE / 1000000000 ))

    echo "=== Launching Dataflow Avro Publisher ==="
    echo "  Job name:     ${JOB_NAME}"
    echo "  Topic:        ${FULL_TOPIC}"
    echo "  Format:       avro (~${AVRO_MSG_SIZE} bytes/msg, schema-determined)"
    echo "  Messages:     ${NUM_MESSAGES}"
    echo "  Total data:   ~${TOTAL_GB} GB"
    echo "  Workers:      ${PUBLISHER_NUM_WORKERS}x ${PUBLISHER_MACHINE_TYPE}"
    echo ""
    echo "  This is a BATCH job. It will auto-terminate after publishing."
    echo ""

    uv run python -m dataflow_pubsub_to_bq.pipeline_publisher \
        --runner=DataflowRunner \
        --project="${PROJECT_ID}" \
        --region="${REGION}" \
        --job_name="${JOB_NAME}" \
        --temp_location="${TEMP_BUCKET}/temp" \
        --staging_location="${TEMP_BUCKET}/staging" \
        --topic="${FULL_TOPIC}" \
        --num_messages="${NUM_MESSAGES}" \
        --format=avro \
        --experiments=use_runner_v2 \
        --sdk_container_image=apache/beam_python3.12_sdk:2.70.0 \
        --extra_packages="${WHEEL_FILE}" \
        --machine_type="${PUBLISHER_MACHINE_TYPE}" \
        --num_workers="${PUBLISHER_NUM_WORKERS}" \
        --max_num_workers="${PUBLISHER_NUM_WORKERS}"

    echo ""
    echo "=== Avro Publisher Job Submitted ==="
    echo ""
    echo "Check status:"
    echo "  ./scripts/run_perf_test.sh publish-status"
    echo ""
    echo "View job at:"
    echo "  https://console.cloud.google.com/dataflow/jobs/${REGION}?project=${PROJECT_ID}"
    echo ""
}

do_publish_status() {
    echo "=== Publisher Job Status ==="
    echo ""
    gcloud dataflow jobs list \
        --project="${PROJECT_ID}" \
        --region="${REGION}" \
        --filter="name~dataflow-perf-publisher" \
        --format="table(id,name,state,createTime)" 2>&1
    echo ""
}

do_job() {
    local LABEL="$1"

    # Validate label
    if [[ -z "${LABEL}" ]]; then
        echo "ERROR: Job label required. Usage: $0 job <a|b|c|d|...>"
        exit 1
    fi

    local SUB_NAME="perf_test_sub_${LABEL}"
    local FULL_SUB="projects/${PROJECT_ID}/subscriptions/${SUB_NAME}"
    local JOB_NAME="dataflow-perf-test-${LABEL}-$(date +%Y%m%d-%H%M%S)"

    # Verify subscription exists
    if ! gcloud pubsub subscriptions describe "${SUB_NAME}" --project="${PROJECT_ID}" 2>/dev/null; then
        echo "ERROR: Subscription ${SUB_NAME} does not exist."
        echo "       Set NUM_CONSUMERS high enough and re-run setup."
        exit 1
    fi

    WHEEL_FILE=$(ls -t dist/*.whl | head -1)
    if [[ -z "${WHEEL_FILE}" ]]; then
        echo "ERROR: No wheel file found. Run './scripts/run_perf_test.sh setup' first."
        exit 1
    fi

    echo "=== Launching Consumer Job ${LABEL^^} ==="
    echo "  Job name:     ${JOB_NAME}"
    echo "  Subscription: ${FULL_SUB}"
    echo "  Workers:      ${CONSUMER_NUM_WORKERS}x ${CONSUMER_MACHINE_TYPE}"
    echo "  At-least-once: ${USE_AT_LEAST_ONCE}"
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
        --machine_type="${CONSUMER_MACHINE_TYPE}" \
        --num_workers="${CONSUMER_NUM_WORKERS}" \
        --max_num_workers="${CONSUMER_NUM_WORKERS}" \
        $( [[ "${USE_AT_LEAST_ONCE}" == "true" ]] && echo "--use_at_least_once" ) \
        --enable_streaming_engine

    echo ""
    echo "=== Consumer Job ${LABEL^^} Submitted ==="
    echo ""
    echo "View job at:"
    echo "  https://console.cloud.google.com/dataflow/jobs/${REGION}?project=${PROJECT_ID}"
    echo ""
}

do_java_setup() {
    echo "=== Building Java Pipeline ==="
    echo ""

    # Check for Maven
    if ! command -v mvn &>/dev/null; then
        echo "ERROR: Maven (mvn) is not installed or not in PATH."
        echo "       Install Maven: https://maven.apache.org/install.html"
        exit 1
    fi

    echo "Building Java project..."
    mvn -f java/pom.xml clean package -DskipTests

    if [[ ! -f "${JAR_FILE}" ]]; then
        echo "ERROR: JAR file not found at ${JAR_FILE}"
        exit 1
    fi

    echo ""
    echo "=== Java Build Complete ==="
    echo "  JAR: ${JAR_FILE}"
    echo ""
    echo "Next: ./scripts/run_perf_test.sh java-job <label>"
    echo ""
}

do_java_job() {
    local LABEL="$1"

    # Validate label
    if [[ -z "${LABEL}" ]]; then
        echo "ERROR: Job label required. Usage: $0 java-job <a|b|c|d|...>"
        exit 1
    fi

    # Verify JAR exists
    if [[ ! -f "${JAR_FILE}" ]]; then
        echo "ERROR: JAR file not found at ${JAR_FILE}"
        echo "       Run './scripts/run_perf_test.sh java-setup' first."
        exit 1
    fi

    local SUB_NAME="perf_test_sub_${LABEL}"
    local FULL_SUB="projects/${PROJECT_ID}/subscriptions/${SUB_NAME}"
    local JOB_NAME="dataflow-perf-java-${LABEL}-$(date +%Y%m%d-%H%M%S)"

    # Verify subscription exists
    if ! gcloud pubsub subscriptions describe "${SUB_NAME}" --project="${PROJECT_ID}" 2>/dev/null; then
        echo "ERROR: Subscription ${SUB_NAME} does not exist."
        echo "       Set NUM_CONSUMERS high enough and re-run setup."
        exit 1
    fi

    echo "=== Launching Java Consumer Job ${LABEL^^} ==="
    echo "  Job name:       ${JOB_NAME}"
    echo "  Subscription:   ${FULL_SUB}"
    echo "  Workers:        ${JAVA_CONSUMER_NUM_WORKERS}x ${JAVA_CONSUMER_MACHINE_TYPE}"
    echo "  Write method:   STORAGE_API_AT_LEAST_ONCE"
    echo "  Connection pool: enabled"
    echo "  BQ table:       ${FULL_TABLE}"
    echo ""

    java -jar "${JAR_FILE}" \
        --runner=DataflowRunner \
        --project="${PROJECT_ID}" \
        --region="${REGION}" \
        --jobName="${JOB_NAME}" \
        --tempLocation="${TEMP_BUCKET}/temp" \
        --stagingLocation="${TEMP_BUCKET}/staging" \
        --subscription="${FULL_SUB}" \
        --outputTable="${FULL_TABLE}" \
        --subscriptionName="${SUB_NAME}" \
        --streaming \
        --experiments=use_runner_v2 \
        --workerMachineType="${JAVA_CONSUMER_MACHINE_TYPE}" \
        --numWorkers="${JAVA_CONSUMER_NUM_WORKERS}" \
        --maxNumWorkers="${JAVA_CONSUMER_NUM_WORKERS}" \
        --enableStreamingEngine \
        --useStorageApiConnectionPool=true

    echo ""
    echo "=== Java Consumer Job ${LABEL^^} Submitted ==="
    echo ""
    echo "View job at:"
    echo "  https://console.cloud.google.com/dataflow/jobs/${REGION}?project=${PROJECT_ID}"
    echo ""
}

do_java_avro_job() {
    local LABEL="$1"

    # Validate label
    if [[ -z "${LABEL}" ]]; then
        echo "ERROR: Job label required. Usage: $0 java-avro-job <a|b|c|d|...>"
        exit 1
    fi

    # Verify JAR exists
    if [[ ! -f "${JAR_FILE}" ]]; then
        echo "ERROR: JAR file not found at ${JAR_FILE}"
        echo "       Run './scripts/run_perf_test.sh setup-avro' first."
        exit 1
    fi

    local SUB_NAME="perf_test_sub_${LABEL}"
    local FULL_SUB="projects/${PROJECT_ID}/subscriptions/${SUB_NAME}"
    local JOB_NAME="dataflow-perf-avro-java-${LABEL}-$(date +%Y%m%d-%H%M%S)"

    # Verify subscription exists
    if ! gcloud pubsub subscriptions describe "${SUB_NAME}" --project="${PROJECT_ID}" 2>/dev/null; then
        echo "ERROR: Subscription ${SUB_NAME} does not exist."
        echo "       Set NUM_CONSUMERS high enough and re-run setup-avro."
        exit 1
    fi

    echo "=== Launching Java Avro Consumer Job ${LABEL^^} ==="
    echo "  Job name:        ${JOB_NAME}"
    echo "  Subscription:    ${FULL_SUB}"
    echo "  Workers:         ${JAVA_CONSUMER_NUM_WORKERS}x ${JAVA_CONSUMER_MACHINE_TYPE}"
    echo "  Write method:    STORAGE_API_AT_LEAST_ONCE"
    echo "  Connection pool: enabled"
    echo "  Dynamic routing: ride_status -> 4 tables"
    echo "  BQ table base:   ${FULL_TABLE}"
    echo ""

    java -cp "${JAR_FILE}" com.johanesalxd.PubSubToBigQueryAvro \
        --runner=DataflowRunner \
        --project="${PROJECT_ID}" \
        --region="${REGION}" \
        --jobName="${JOB_NAME}" \
        --tempLocation="${TEMP_BUCKET}/temp" \
        --stagingLocation="${TEMP_BUCKET}/staging" \
        --subscription="${FULL_SUB}" \
        --outputTableBase="${FULL_TABLE}" \
        --subscriptionName="${SUB_NAME}" \
        --streaming \
        --experiments=use_runner_v2 \
        --workerMachineType="${JAVA_CONSUMER_MACHINE_TYPE}" \
        --numWorkers="${JAVA_CONSUMER_NUM_WORKERS}" \
        --maxNumWorkers="${JAVA_CONSUMER_NUM_WORKERS}" \
        --enableStreamingEngine \
        --useStorageApiConnectionPool=true

    echo ""
    echo "=== Java Avro Consumer Job ${LABEL^^} Submitted ==="
    echo ""
    echo "View job at:"
    echo "  https://console.cloud.google.com/dataflow/jobs/${REGION}?project=${PROJECT_ID}"
    echo ""
}

do_monitor() {
    print_config
    echo "--- Console Links ---"
    echo ""
    echo "1. Dataflow Jobs:"
    echo "   https://console.cloud.google.com/dataflow/jobs?project=${PROJECT_ID}&region=${REGION}"
    echo ""
    echo "2. Pub/Sub Subscriptions (quickest backlog check):"
    echo "   https://console.cloud.google.com/cloudpubsub/subscription/list?project=${PROJECT_ID}"
    echo ""
    echo "3. BigQuery Table:"
    echo "   https://console.cloud.google.com/bigquery?project=${PROJECT_ID}&ws=!1m5!1m4!4m3!1s${PROJECT_ID}!2s${BIGQUERY_DATASET}!3s${BIGQUERY_TABLE}"
    echo ""
    echo "4. BQ Storage Write API Quota (throughput):"
    echo "   https://console.cloud.google.com/iam-admin/quotas?project=${PROJECT_ID}&metric=bigquerystorage.googleapis.com/write/append_bytes"
    echo "   Quota name: AppendBytesThroughputPerProjectRegion"
    echo ""
    echo "5. BQ Storage Write API Quota (connections):"
    echo "   https://console.cloud.google.com/iam-admin/quotas?project=${PROJECT_ID}&metric=bigquerystorage.googleapis.com/write/max_active_streams"
    echo "   Quota name: ConcurrentWriteConnectionsPerProjectRegion (limit: 1,000)"
    echo ""
    echo "--- Cloud Monitoring Metrics (Metrics Explorer) ---"
    echo ""
    echo "Open Metrics Explorer:"
    echo "  https://console.cloud.google.com/monitoring/metrics-explorer?project=${PROJECT_ID}"
    echo ""
    echo "Search for these metrics:"
    echo ""
    echo "1. BQ Storage Write API throughput (bytes written):"
    echo "   Metric:      bigquerystorage.googleapis.com/write/uploaded_bytes_count"
    echo "   Aggregation: Sum, 1 minute"
    echo "   Note:        For Dataflow-specific view, use:"
    echo "                bigquerystorage.googleapis.com/dataflow_write/uploaded_bytes_count"
    echo ""
    echo "2. BQ concurrent connections:"
    echo "   Metric:      bigquerystorage.googleapis.com/write/concurrent_connections"
    echo "   Aggregation: Sum, 1 minute"
    echo ""
    echo "3. Pub/Sub subscription backlog:"
    echo "   Metric:      pubsub.googleapis.com/subscription/num_unacked_messages_by_region"
    echo "   Filter:      subscription_id = perf_test_sub_a, perf_test_sub_b, ..."
    echo ""
    echo "--- Dataflow Console (no setup needed) ---"
    echo ""
    echo "4. Worker CPU:"
    echo "   Dataflow > click job > Job Metrics tab > look at CPU Utilization"
    echo ""
    echo "5. BQ write wall time:"
    echo "   Dataflow > click job > Execution Details tab > click WriteToBigQuery stage"
    echo ""
    echo "--- What to Look For ---"
    echo ""
    echo "Phase 1 (single job):"
    echo "  - Worker CPU < 30% = I/O bound (expected)"
    echo "  - Pub/Sub backlog stable or near zero = pipeline keeping up"
    echo "  - BQ write throughput ~160 MB/s per job"
    echo ""
    echo "Phase 2 (two jobs, same table):"
    echo "  - Sub A backlog grows after Job B starts = noisy neighbor confirmed"
    echo "  - Job A system lag increases = BQ contention slowing pipeline"
    echo "  - Combined BQ write < 320 MB/s = shared resource limit"
    echo "  - Quota page shows < 100% usage = not quota-bound (per-table contention)"
    echo "  - Connections near 1,000 = connection-bound, not throughput-bound"
    echo ""
}

do_dry_run() {
    echo "=== Dry Run -- Validating Message Sizes ==="
    echo ""
    uv run python -m dataflow_pubsub_to_bq.pipeline_publisher \
        --num_messages="${NUM_MESSAGES}" \
        --message_size_bytes="${MESSAGE_SIZE_BYTES}" \
        --validate_only
}

do_dry_run_avro() {
    echo "=== Dry Run -- Validating Avro Message Sizes ==="
    echo ""
    uv run python -m dataflow_pubsub_to_bq.pipeline_publisher \
        --num_messages="${NUM_MESSAGES}" \
        --format=avro \
        --validate_only
}

# ===================================================================
# Main dispatch
# ===================================================================

COMMAND="${1:-help}"

case "${COMMAND}" in
    setup)
        do_setup
        ;;
    setup-avro)
        do_setup_avro
        ;;
    publish)
        do_publish
        ;;
    publish-avro)
        do_publish_avro
        ;;
    publish-status)
        do_publish_status
        ;;
    job)
        do_job "${2}"
        ;;
    java-setup)
        do_java_setup
        ;;
    java-job)
        do_java_job "${2}"
        ;;
    java-avro-job)
        do_java_avro_job "${2}"
        ;;
    monitor)
        do_monitor
        ;;
    dry-run)
        do_dry_run
        ;;
    dry-run-avro)
        do_dry_run_avro
        ;;
    *)
        echo "Usage: $0 <command> [args]"
        echo ""
        echo "Commands (JSON pipeline -- rounds 1-4):"
        echo "  setup            Create GCP resources (topic, subs, BQ JSON table)"
        echo "  publish          Launch JSON publisher (batch, auto-terminates)"
        echo "  publish-status   Check publisher job status"
        echo "  job <label>      Launch Python consumer Job <label> (e.g., job a)"
        echo "  java-setup       Build Java pipeline JAR (requires Maven)"
        echo "  java-job <label> Launch Java JSON consumer Job <label>"
        echo "  dry-run          Validate JSON message sizes"
        echo ""
        echo "Commands (Avro pipeline -- round 5):"
        echo "  setup-avro           Create GCP resources (topic, subs, 4 typed BQ tables + DLQ)"
        echo "  publish-avro         Launch Avro publisher (batch, ~85 bytes/msg)"
        echo "  java-avro-job <label> Launch Java Avro consumer with dynamic routing"
        echo "  dry-run-avro         Validate Avro message sizes and expansion ratio"
        echo ""
        echo "Common:"
        echo "  monitor          Print monitoring URLs and metric names"
        echo ""
        echo "Typical workflow (Avro + dynamic routing -- round 5):"
        echo "  1. ./scripts/run_perf_test.sh setup-avro"
        echo "  2. ./scripts/run_perf_test.sh dry-run-avro          # verify sizes"
        echo "  3. ./scripts/run_perf_test.sh publish-avro"
        echo "  4. ./scripts/run_perf_test.sh publish-status        # wait for backlog"
        echo "  5. ./scripts/run_perf_test.sh java-avro-job a       # Phase 1: baseline"
        echo "  6. Wait 10 min for steady state"
        echo "  7. ./scripts/run_perf_test.sh java-avro-job b       # Phase 2: noisy neighbor"
        echo "  8. ./scripts/run_perf_test.sh monitor"
        echo "  9. ./scripts/cleanup_perf_test.sh"
        echo ""
        echo "Typical workflow (JSON -- rounds 1-4):"
        echo "  1. ./scripts/run_perf_test.sh setup"
        echo "  2. ./scripts/run_perf_test.sh publish"
        echo "  3. ./scripts/run_perf_test.sh publish-status        # wait for Done"
        echo "  4. ./scripts/run_perf_test.sh job a                 # Phase 1 baseline"
        echo "  5. Wait 10 min for steady state"
        echo "  6. ./scripts/run_perf_test.sh job b                 # Phase 2 noisy neighbor"
        echo "  7. ./scripts/run_perf_test.sh monitor"
        echo "  8. ./scripts/cleanup_perf_test.sh"
        exit 1
        ;;
esac

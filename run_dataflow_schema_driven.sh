#!/bin/bash

# Schema-Driven Dataflow Pub/Sub to BigQuery Pipeline
# This script deploys a pipeline that fetches its schema from the Pub/Sub Schema Registry.
# The .avsc file is only used to create the schema in the registry.
# After that, all consumers (this script, the pipeline) fetch from the registry.
#
# On subsequent runs (e.g., after a v2 schema commit), the script re-fetches the schema
# from the registry and updates the BQ table automatically. No code changes needed.

set -e

# --- Configuration ---
PROJECT_ID="johanesa-playground-326616"
REGION="us-central1"
TEMP_BUCKET="gs://johanesa-playground-326616-dataflow-bucket"
BIGQUERY_DATASET="demo_dataset"
BIGQUERY_TABLE="taxi_events_schema"
SCHEMA_NAME="taxi-ride-schema"
SCHEMA_TOPIC="taxi_telemetry_schema"
SCHEMA_SUBSCRIPTION="taxi_telemetry_schema_sub"
SOURCE_SUBSCRIPTION="taxi_telemetry_schema_source"
PUBLIC_TOPIC="projects/pubsub-public-data/topics/taxirides-realtime"
JOB_NAME="dataflow-schema-driven-$(date +%Y%m%d-%H%M%S)"

# Full resource paths
FULL_SCHEMA="projects/${PROJECT_ID}/schemas/${SCHEMA_NAME}"
FULL_TOPIC="projects/${PROJECT_ID}/topics/${SCHEMA_TOPIC}"
FULL_SUBSCRIPTION="projects/${PROJECT_ID}/subscriptions/${SCHEMA_SUBSCRIPTION}"
FULL_TABLE="${PROJECT_ID}:${BIGQUERY_DATASET}.${BIGQUERY_TABLE}"

# --- Main Script ---

echo "=== Schema-Driven Dataflow Pipeline ==="
echo "Project ID: ${PROJECT_ID}"
echo "Region: ${REGION}"
echo "Job Name: ${JOB_NAME}"
echo "Schema: ${FULL_SCHEMA}"
echo "Topic: ${FULL_TOPIC}"
echo "BigQuery Table: ${FULL_TABLE}"
echo ""

# 1. Check/Create GCS bucket
echo "Checking GCS bucket..."
if ! gsutil ls "${TEMP_BUCKET}" 2>/dev/null; then
    echo "Creating GCS bucket..."
    gsutil mb -p "${PROJECT_ID}" -l "${REGION}" "${TEMP_BUCKET}"
else
    echo "GCS bucket already exists."
fi

# 2. Check/Create BigQuery dataset
echo "Checking BigQuery dataset..."
if ! bq show --dataset "${PROJECT_ID}:${BIGQUERY_DATASET}" 2>/dev/null; then
    echo "Creating BigQuery dataset..."
    bq mk --dataset --location="${REGION}" "${PROJECT_ID}:${BIGQUERY_DATASET}"
else
    echo "BigQuery dataset already exists."
fi

# 3. Check/Create Pub/Sub Schema (uses local .avsc file for initial creation only)
echo "Checking Pub/Sub schema..."
if ! gcloud pubsub schemas describe "${SCHEMA_NAME}" --project="${PROJECT_ID}" 2>/dev/null; then
    echo "Creating Pub/Sub schema from schemas/taxi_ride_v1.avsc..."
    gcloud pubsub schemas create "${SCHEMA_NAME}" \
        --project="${PROJECT_ID}" \
        --type=avro \
        --definition-file=schemas/taxi_ride_v1.avsc
else
    echo "Pub/Sub schema already exists."
fi

# 4. Check/Create schema-enabled Topic
echo "Checking schema-enabled topic..."
if ! gcloud pubsub topics describe "${SCHEMA_TOPIC}" --project="${PROJECT_ID}" 2>/dev/null; then
    echo "Creating schema-enabled topic..."
    gcloud pubsub topics create "${SCHEMA_TOPIC}" \
        --project="${PROJECT_ID}" \
        --schema="${SCHEMA_NAME}" \
        --message-encoding=JSON
else
    echo "Schema-enabled topic already exists."
fi

# 5. Check/Create pipeline subscription (on schema topic)
echo "Checking pipeline subscription..."
if ! gcloud pubsub subscriptions describe "${SCHEMA_SUBSCRIPTION}" --project="${PROJECT_ID}" 2>/dev/null; then
    echo "Creating pipeline subscription..."
    gcloud pubsub subscriptions create "${SCHEMA_SUBSCRIPTION}" \
        --project="${PROJECT_ID}" \
        --topic="${SCHEMA_TOPIC}"
else
    echo "Pipeline subscription already exists."
fi

# 6. Check/Create source subscription (on public taxi topic, for mirror publisher)
echo "Checking source subscription..."
if ! gcloud pubsub subscriptions describe "${SOURCE_SUBSCRIPTION}" --project="${PROJECT_ID}" 2>/dev/null; then
    echo "Creating source subscription on public taxi topic..."
    gcloud pubsub subscriptions create "${SOURCE_SUBSCRIPTION}" \
        --project="${PROJECT_ID}" \
        --topic="${PUBLIC_TOPIC}"
else
    echo "Source subscription already exists."
fi

# 7. Generate BQ schema from Pub/Sub Schema Registry (NOT from local .avsc file)
echo ""
echo "Fetching schema from registry and generating BQ schema..."
BQ_SCHEMA_LINE=$(uv run python schemas/generate_bq_schema.py --schema="${FULL_SCHEMA}")
echo "BQ schema: ${BQ_SCHEMA_LINE}"
echo ""

# 8. Check/Create or update BigQuery table with schema from registry
echo "Checking BigQuery table..."
if ! bq show "${FULL_TABLE}" 2>/dev/null; then
    echo "Creating BigQuery table with schema from registry..."
    bq mk \
        --table \
        --time_partitioning_field=publish_time \
        --time_partitioning_type=DAY \
        --clustering_fields=publish_time \
        --schema="${BQ_SCHEMA_LINE}" \
        "${FULL_TABLE}"
else
    echo "BigQuery table already exists. Updating schema..."
    bq update \
        --schema="${BQ_SCHEMA_LINE}" \
        "${FULL_TABLE}" || echo "Schema update skipped (no new columns)."
fi

# 9. Install Python dependencies
echo ""
echo "Installing Python dependencies..."
uv sync

# 10. Build package wheel for Dataflow workers
echo "Building package wheel for Dataflow workers..."
uv build --wheel
WHEEL_FILE=$(ls -t dist/*.whl | head -1)
echo "Built wheel: ${WHEEL_FILE}"

# 11. Submit pipeline to Dataflow
echo ""
echo "Submitting schema-driven pipeline to Dataflow..."
uv run python -m dataflow_pubsub_to_bq.pipeline_schema_driven \
    --runner=DataflowRunner \
    --project="${PROJECT_ID}" \
    --region="${REGION}" \
    --job_name="${JOB_NAME}" \
    --temp_location="${TEMP_BUCKET}/temp" \
    --staging_location="${TEMP_BUCKET}/staging" \
    --subscription="${FULL_SUBSCRIPTION}" \
    --output_table="${FULL_TABLE}" \
    --subscription_name="${SCHEMA_SUBSCRIPTION}" \
    --pubsub_schema="${FULL_SCHEMA}" \
    --streaming \
    --experiments=use_runner_v2 \
    --sdk_container_image=apache/beam_python3.12_sdk:2.70.0 \
    --extra_packages="${WHEEL_FILE}" \
    --machine_type=n2-standard-4 \
    --num_workers=5 \
    --max_num_workers=5 \
    --enable_streaming_engine

echo ""
echo "=== Job Submitted Successfully ==="
echo "Pipeline has been submitted to Dataflow."
echo ""
echo "View job at:"
echo "https://console.cloud.google.com/dataflow/jobs/${REGION}/${JOB_NAME}?project=${PROJECT_ID}"
echo ""
echo "Remember to start the mirror publisher separately:"
echo "  python publish_to_schema_topic.py \\"
echo "      --project=${PROJECT_ID} \\"
echo "      --source-subscription=${SOURCE_SUBSCRIPTION} \\"
echo "      --target-topic=${FULL_TOPIC}"
echo ""

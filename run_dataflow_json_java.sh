#!/bin/bash

# Dataflow Pub/Sub to BigQuery Pipeline (Java Version - JSON Column)
# This script runs the pipeline on Dataflow with specific scaling test configuration
# - Reads from Pub/Sub subscription (public taxi data)
# - Writes to BigQuery table with JSON column type
# - Uses Runner V2 with fixed 5 workers (n2-standard-4) for scaling test

set -e

# --- Configuration ---
PROJECT_ID="your-project-id"
REGION="us-central1"
TEMP_BUCKET="gs://your-gcs-bucket"
BIGQUERY_DATASET="demo_dataset"
BIGQUERY_TABLE="taxi_events_json"
SUBSCRIPTION_NAME="taxi_telemetry_json"
PUBLIC_TOPIC="projects/pubsub-public-data/topics/taxirides-realtime"
JOB_NAME="dataflow-pubsub-to-bq-json-java-$(date +%Y%m%d-%H%M%S)"

# Full resource paths
FULL_SUBSCRIPTION="projects/${PROJECT_ID}/subscriptions/${SUBSCRIPTION_NAME}"
FULL_TABLE="${PROJECT_ID}:${BIGQUERY_DATASET}.${BIGQUERY_TABLE}"

# --- Main Script ---

echo "=== Dataflow Pub/Sub to BigQuery Pipeline (Java JSON Version) ==="
echo "Project ID: ${PROJECT_ID}"
echo "Region: ${REGION}"
echo "Job Name: ${JOB_NAME}"
echo "Subscription: ${SUBSCRIPTION_NAME}"
echo "BigQuery Table: ${FULL_TABLE}"
echo ""
echo "Scaling Test Configuration (Matches Python):"
echo "  - Machine Type: n2-standard-4"
echo "  - Workers: 5 (fixed)"
echo "  - BigQuery Write: Storage Write API with 1-second micro-batching"
echo ""

# 1. Check/Create GCS bucket
echo "Checking GCS bucket..."
if ! gsutil ls ${TEMP_BUCKET} 2>/dev/null; then
    echo "Creating GCS bucket..."
    gsutil mb -p ${PROJECT_ID} -l ${REGION} ${TEMP_BUCKET}
else
    echo "GCS bucket already exists."
fi

# 2. Check/Create BigQuery dataset
echo "Checking BigQuery dataset..."
if ! bq show --dataset ${PROJECT_ID}:${BIGQUERY_DATASET} 2>/dev/null; then
    echo "Creating BigQuery dataset..."
    bq mk --dataset --location=${REGION} ${PROJECT_ID}:${BIGQUERY_DATASET}
else
    echo "BigQuery dataset already exists."
fi

# 3. Check/Create BigQuery table with schema (JSON Column)
# Note: Using the same schema definition as the Python script for consistency
echo "Checking BigQuery table..."
if ! bq show ${FULL_TABLE} 2>/dev/null; then
    echo "Creating BigQuery table with JSON schema..."
    bq mk \
        --table \
        --time_partitioning_field=publish_time \
        --time_partitioning_type=DAY \
        --clustering_fields=publish_time \
        --schema=subscription_name:STRING,message_id:STRING,publish_time:TIMESTAMP,processing_time:TIMESTAMP,attributes:STRING,payload:JSON \
        ${FULL_TABLE}
else
    echo "BigQuery table already exists."
fi

# 4. Check/Create BigQuery DLQ table
echo "Checking BigQuery DLQ table..."
if ! bq show ${FULL_TABLE}_dlq 2>/dev/null; then
    echo "Creating BigQuery DLQ table..."
    bq mk \
        --table \
        --time_partitioning_field=processing_time \
        --time_partitioning_type=DAY \
        --schema=processing_time:TIMESTAMP,error_message:STRING,stack_trace:STRING,original_payload:STRING,subscription_name:STRING \
        ${FULL_TABLE}_dlq
else
    echo "BigQuery DLQ table already exists."
fi

# 5. Check/Create Pub/Sub subscription
echo "Checking Pub/Sub subscription..."
if ! gcloud pubsub subscriptions describe ${SUBSCRIPTION_NAME} --project=${PROJECT_ID} 2>/dev/null; then
    echo "Creating Pub/Sub subscription..."
    gcloud pubsub subscriptions create ${SUBSCRIPTION_NAME} \
        --project=${PROJECT_ID} \
        --topic=${PUBLIC_TOPIC}
else
    echo "Pub/Sub subscription already exists."
fi

# 5. Build Java Project
echo "Building Java project..."
mvn -f java/pom.xml clean package -DskipTests

JAR_FILE="java/target/dataflow-pubsub-to-bq-json-1.0-SNAPSHOT.jar"
if [ ! -f "$JAR_FILE" ]; then
    echo "Error: JAR file not found at $JAR_FILE"
    exit 1
fi
echo "Built JAR: ${JAR_FILE}"

# 7. Submit pipeline to Dataflow
echo ""
echo "Submitting pipeline to Dataflow with Runner V2..."

java -jar ${JAR_FILE} \
    --runner=DataflowRunner \
    --project=${PROJECT_ID} \
    --region=${REGION} \
    --jobName=${JOB_NAME} \
    --tempLocation=${TEMP_BUCKET}/temp \
    --stagingLocation=${TEMP_BUCKET}/staging \
    --subscription=${FULL_SUBSCRIPTION} \
    --outputTable=${FULL_TABLE} \
    --subscriptionName=${SUBSCRIPTION_NAME} \
    --streaming \
    --experiments=use_runner_v2 \
    --workerMachineType=n2-standard-4 \
    --numWorkers=5 \
    --maxNumWorkers=5 \
    --enableStreamingEngine

echo ""
echo "=== Job Submitted Successfully ==="
echo "The pipeline has been submitted to Dataflow and is starting up..."
echo ""
echo "View job at:"
echo "https://console.cloud.google.com/dataflow/jobs/${REGION}/${JOB_NAME}?project=${PROJECT_ID}"
echo ""
echo "Monitor BigQuery table at:"
echo "https://console.cloud.google.com/bigquery?project=${PROJECT_ID}&ws=!1m5!1m4!4m3!1s${PROJECT_ID}!2s${BIGQUERY_DATASET}!3s${BIGQUERY_TABLE}"

#!/bin/bash

# Mirror Publisher for Schema-Driven Pipeline
# Bridges the public taxi topic to the schema-enabled topic.
# Pure pass-through relay -- re-publishes messages as-is.
#
# Run this in a separate terminal alongside the Dataflow pipeline.
# Press Ctrl+C to stop gracefully.

set -e

# --- Configuration (must match run_dataflow_schema_driven.sh) ---
PROJECT_ID="your-project-id"
SCHEMA_TOPIC="taxi_telemetry_schema"
SOURCE_SUBSCRIPTION="taxi_telemetry_schema_source"

# Full resource paths
FULL_TOPIC="projects/${PROJECT_ID}/topics/${SCHEMA_TOPIC}"
FULL_SOURCE_SUBSCRIPTION="projects/${PROJECT_ID}/subscriptions/${SOURCE_SUBSCRIPTION}"

echo "=== Mirror Publisher ==="
echo "Project ID: ${PROJECT_ID}"
echo "Source: ${FULL_SOURCE_SUBSCRIPTION}"
echo "Target: ${FULL_TOPIC}"
echo ""
echo "Press Ctrl+C to stop."
echo ""

uv run python scripts/publish_to_schema_topic.py \
    --project="${PROJECT_ID}" \
    --source-subscription="${FULL_SOURCE_SUBSCRIPTION}" \
    --target-topic="${FULL_TOPIC}"

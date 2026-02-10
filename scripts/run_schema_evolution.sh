#!/bin/bash

# Schema Evolution Script - Phase 2
#
# Evolves the Pub/Sub schema from v1 to v2, demonstrates governance enforcement,
# and starts the v2 mirror publisher.
#
# This script runs in its own terminal (Terminal 3) alongside:
#   Terminal 1: ./run_dataflow_schema_driven.sh (pipeline deployment)
#   Terminal 2: ./scripts/run_mirror_publisher.sh (v1 mirror publisher)
#
# Steps:
#   1. Create Sub B with SMT on the public taxi topic
#   2. Governance proof: demonstrate what schema validation enforces
#   3. Commit v2 schema revision
#   4. Update topic revision range to accept v1 + v2
#   5. PAUSE: user re-runs ./run_dataflow_schema_driven.sh in Terminal 1
#   6. Start v2 mirror publisher

set -e

# --- Configuration (must match run_dataflow_schema_driven.sh) ---
PROJECT_ID="your-project-id"
REGION="us-central1"
SCHEMA_NAME="taxi-ride-schema"
SCHEMA_TOPIC="taxi_telemetry_schema"
V2_SOURCE_SUBSCRIPTION="taxi_telemetry_schema_v2_source"
PUBLIC_TOPIC="projects/pubsub-public-data/topics/taxirides-realtime"
SMT_FILE="scripts/enrich_taxi_ride.yaml"

# Full resource paths
FULL_TOPIC="projects/${PROJECT_ID}/topics/${SCHEMA_TOPIC}"
FULL_V2_SOURCE="projects/${PROJECT_ID}/subscriptions/${V2_SOURCE_SUBSCRIPTION}"

echo "=== Schema Evolution: Phase 2 ==="
echo "Project ID: ${PROJECT_ID}"
echo "Schema: ${SCHEMA_NAME}"
echo "Topic: ${FULL_TOPIC}"
echo "V2 Source Subscription: ${V2_SOURCE_SUBSCRIPTION}"
echo ""

# ===================================================================
# Step 1: Create Sub B with SMT on public taxi topic
# ===================================================================
echo "--- Step 1: Create Sub B with SMT ---"
if gcloud pubsub subscriptions describe "${V2_SOURCE_SUBSCRIPTION}" --project="${PROJECT_ID}" 2>/dev/null; then
    echo "Subscription ${V2_SOURCE_SUBSCRIPTION} already exists."
else
    echo "Creating subscription with SMT..."
    gcloud pubsub subscriptions create "${V2_SOURCE_SUBSCRIPTION}" \
        --project="${PROJECT_ID}" \
        --topic="${PUBLIC_TOPIC}" \
        --message-transforms-file="${SMT_FILE}"
    echo "Created subscription: ${V2_SOURCE_SUBSCRIPTION}"
fi
echo ""

# ===================================================================
# Step 2: Governance proof -- schema enforcement
# ===================================================================
echo "--- Step 2: Governance Proof -- Schema Enforcement ---"
echo "Demonstrating what the Pub/Sub Schema Registry enforces."
echo "Using 'gcloud pubsub schemas validate-message' to test messages"
echo "against the v1 schema."
echo ""

SCHEMA_PATH="projects/${PROJECT_ID}/schemas/${SCHEMA_NAME}"

# 2a. Valid v1 message -- should pass
echo "  2a. Valid v1 message (all required fields, correct types):"
VALID_MSG='{"ride_id":"test-123","point_idx":1,"latitude":40.76,"longitude":-73.97,"timestamp":"2026-02-10T08:14:17.61844-05:00","meter_reading":4.81,"meter_increment":0.046,"ride_status":"enroute","passenger_count":2}'
if gcloud pubsub schemas validate-message \
    --schema-name="${SCHEMA_PATH}" \
    --message-encoding=json \
    --message="${VALID_MSG}" 2>&1; then
    echo "      RESULT: Passed (as expected)"
else
    echo "      RESULT: Failed (unexpected)"
fi
echo ""

# 2b. Missing required field -- should fail
echo "  2b. Missing required field (no passenger_count):"
MISSING_FIELD_MSG='{"ride_id":"test-123","point_idx":1,"latitude":40.76,"longitude":-73.97,"timestamp":"2026-02-10T08:14:17.61844-05:00","meter_reading":4.81,"meter_increment":0.046,"ride_status":"enroute"}'
if gcloud pubsub schemas validate-message \
    --schema-name="${SCHEMA_PATH}" \
    --message-encoding=json \
    --message="${MISSING_FIELD_MSG}" 2>&1; then
    echo "      RESULT: Passed (unexpected)"
else
    echo "      RESULT: Rejected (as expected -- required field missing)"
fi
echo ""

# 2c. Wrong type -- should fail
echo "  2c. Wrong type (point_idx as string instead of int):"
WRONG_TYPE_MSG='{"ride_id":"test-123","point_idx":"NOT_AN_INT","latitude":40.76,"longitude":-73.97,"timestamp":"2026-02-10T08:14:17.61844-05:00","meter_reading":4.81,"meter_increment":0.046,"ride_status":"enroute","passenger_count":2}'
if gcloud pubsub schemas validate-message \
    --schema-name="${SCHEMA_PATH}" \
    --message-encoding=json \
    --message="${WRONG_TYPE_MSG}" 2>&1; then
    echo "      RESULT: Passed (unexpected)"
else
    echo "      RESULT: Rejected (as expected -- type mismatch)"
fi
echo ""

# 2d. Extra fields (v2 message) -- should pass
echo "  2d. Extra fields (v2 message with enrichment_timestamp + region):"
V2_MSG='{"ride_id":"test-123","point_idx":1,"latitude":40.76,"longitude":-73.97,"timestamp":"2026-02-10T08:14:17.61844-05:00","meter_reading":4.81,"meter_increment":0.046,"ride_status":"enroute","passenger_count":2,"enrichment_timestamp":"2026-02-10T13:13:36.521Z","region":"midtown"}'
if gcloud pubsub schemas validate-message \
    --schema-name="${SCHEMA_PATH}" \
    --message-encoding=json \
    --message="${V2_MSG}" 2>&1; then
    echo "      RESULT: Passed (as expected -- Avro ignores extra fields)"
else
    echo "      RESULT: Rejected (unexpected)"
fi
echo ""

echo "Governance proof complete."
echo ""
echo "Summary: The schema registry enforces STRUCTURAL contracts:"
echo "  - Required fields must be present (2b rejected)"
echo "  - Fields must have correct types (2c rejected)"
echo "  - Extra fields are allowed per Avro forward compatibility (2d passed)"
echo ""
echo "This means v2 messages (with extra fields) can be published to a"
echo "v1-only topic without errors. The extra fields are stored but ignored"
echo "by the v1 pipeline. After upgrading to v2, the pipeline extracts them."
echo ""
read -p "Press Enter to continue with schema evolution..."

# ===================================================================
# Step 3: Commit v2 schema revision
# ===================================================================
echo ""
echo "--- Step 3: Commit v2 Schema Revision ---"
gcloud pubsub schemas commit "${SCHEMA_NAME}" \
    --project="${PROJECT_ID}" \
    --type=avro \
    --definition-file=schemas/taxi_ride_v2.avsc
echo "v2 schema revision committed."
echo ""

# ===================================================================
# Step 4: Update topic revision range
# ===================================================================
echo "--- Step 4: Update Topic Revision Range ---"
echo "Fetching schema revision IDs..."

# Get all revision IDs (oldest first)
REVISION_IDS=$(gcloud pubsub schemas list-revisions "${SCHEMA_NAME}" \
    --project="${PROJECT_ID}" \
    --format="value(revisionId)" \
    --sort-by="revisionCreateTime")

V1_REVISION_ID=$(echo "${REVISION_IDS}" | head -1)
V2_REVISION_ID=$(echo "${REVISION_IDS}" | tail -1)

echo "  v1 revision: ${V1_REVISION_ID}"
echo "  v2 revision: ${V2_REVISION_ID}"
echo ""

echo "Updating topic to accept revisions ${V1_REVISION_ID} through ${V2_REVISION_ID}..."
gcloud pubsub topics update "${SCHEMA_TOPIC}" \
    --project="${PROJECT_ID}" \
    --schema="${SCHEMA_NAME}" \
    --first-revision-id="${V1_REVISION_ID}" \
    --last-revision-id="${V2_REVISION_ID}" \
    --message-encoding=JSON
echo "Topic revision range updated. Both v1 and v2 messages are now accepted."
echo ""

# ===================================================================
# Step 5: Pause -- user re-runs deployment script
# ===================================================================
echo "============================================================"
echo "  Schema evolution complete."
echo "  The topic now accepts both v1 and v2 messages."
echo ""
echo "  ACTION REQUIRED:"
echo "    Re-run ./run_dataflow_schema_driven.sh in Terminal 1."
echo "    This will:"
echo "      - Auto-cancel the current Dataflow job"
echo "      - Update the BigQuery table with v2 columns"
echo "      - Submit a new pipeline that picks up the v2 schema"
echo ""
echo "  Wait for the new job to appear in the Dataflow console"
echo "  before continuing."
echo "============================================================"
echo ""
read -p "Press Enter after the new pipeline has been submitted..."

# ===================================================================
# Step 6: Start v2 mirror publisher
# ===================================================================
echo ""
echo "--- Step 6: Starting v2 Mirror Publisher ---"
echo "Source: ${FULL_V2_SOURCE}"
echo "Target: ${FULL_TOPIC}"
echo ""
echo "The schema topic will now receive a mix of:"
echo "  - v1 messages (from Terminal 2, Sub A, no enrichment)"
echo "  - v2 messages (from this terminal, Sub B, with enrichment)"
echo ""
echo "Press Ctrl+C to stop."
echo ""

uv run python scripts/publish_to_schema_topic.py \
    --project="${PROJECT_ID}" \
    --source-subscription="${FULL_V2_SOURCE}" \
    --target-topic="${FULL_TOPIC}"

# Schema Evolution with Pub/Sub Schema Registry

This document describes the design and implementation plan for demonstrating Pub/Sub Schema Registry
with Avro schema evolution in a Dataflow streaming pipeline. The goal is to show how schema governance
works at the messaging layer and how a schema-driven pipeline automatically adapts to schema changes
with zero code modifications -- only a job restart.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Phase 1: Schema v1](#phase-1-schema-v1)
  - [Avro Schema Definition (v1)](#avro-schema-definition-v1)
  - [Avro-to-BigQuery Type Mapping](#avro-to-bigquery-type-mapping)
  - [Infrastructure Setup](#infrastructure-setup)
  - [Mirror Publisher](#mirror-publisher)
  - [Schema-Driven Dataflow Pipeline](#schema-driven-dataflow-pipeline)
  - [BigQuery Table Schema](#bigquery-table-schema)
  - [Files to Create](#files-to-create)
- [Phase 2: Schema v2 Evolution (Future)](#phase-2-schema-v2-evolution-future)
  - [Avro Schema Definition (v2)](#avro-schema-definition-v2)
  - [Subscription SMT for Enrichment](#subscription-smt-for-enrichment)
  - [Steps to Evolve](#steps-to-evolve)
  - [Demo Narrative](#demo-narrative)
  - [Validation Queries](#validation-queries)
- [Schema Governance Enforcement](#schema-governance-enforcement)
  - [Where Validation Lives](#where-validation-lives)
  - [What Happens When v2 Data Hits a v1 Schema](#what-happens-when-v2-data-hits-a-v1-schema)
  - [Ordering Dependency](#ordering-dependency)
- [Key Design Decisions](#key-design-decisions)
  - [Why Schema-Driven (Not Hardcoded)](#why-schema-driven-not-hardcoded)
  - [Why Avro Over Protocol Buffers](#why-avro-over-protocol-buffers)
  - [Why JSON Encoding (Not Binary)](#why-json-encoding-not-binary)
  - [Schema Registry as Validation vs Encoding](#schema-registry-as-validation-vs-encoding)
  - [Avro Logical Types for Timestamps](#avro-logical-types-for-timestamps)
- [Pub/Sub Schema Registry Concepts](#pubsub-schema-registry-concepts)
  - [Schema Revisions](#schema-revisions)
  - [Publish-Time Validation](#publish-time-validation)
  - [Schema Metadata Attributes](#schema-metadata-attributes)
  - [Single Message Transforms (SMTs)](#single-message-transforms-smts)

## Overview

The existing pipelines in this repository (`pipeline.py`, `pipeline_json.py`) consume from a public
Pub/Sub topic (`projects/pubsub-public-data/topics/taxirides-realtime`) and parse JSON messages
in application code. Field extraction and BigQuery schema are hardcoded -- adding a new field
requires code changes, redeployment, and `ALTER TABLE` on BigQuery.

This new pipeline variant (`pipeline_schema_driven.py`) introduces **Pub/Sub Schema Registry**
with a **schema-driven** architecture:

- An Avro schema defines the message contract in the Pub/Sub Schema Registry.
- Pub/Sub validates every message at **publish time** -- bad data is rejected before entering
  the system. Because validation happens at the topic level, the pipeline does not need a
  dead letter queue (DLQ). Messages that reach the pipeline are guaranteed to conform to the schema.
- The Dataflow pipeline **fetches the Avro schema from the registry at startup** and dynamically
  generates both the BigQuery table schema and the field extraction logic from it. The `.avsc`
  files in the repository are only used to upload schema definitions to the registry; after that,
  the registry is the single source of truth for all downstream consumers.
- Schema evolution requires **zero pipeline code changes** -- commit a new schema revision,
  update the BQ table, and restart the Dataflow job. The pipeline automatically adapts.

The demonstration has two phases:

1. **Phase 1 (v1):** Establish the schema-driven pipeline with the original taxi ride fields.
2. **Phase 2 (v2):** Commit a new schema revision with additional fields, use Pub/Sub Subscription
   SMTs to enrich messages, and show that the pipeline handles mixed v1/v2 messages after a restart
   with no code changes.

## Architecture

### Phase 1: v1 Only

```
┌──────────────────────────────┐     ┌────────────────────┐
│ Public Taxi Topic            │────>│ Sub A (no SMT)     │
│ (pubsub-public-data)         │     │ (source for v1)    │
└──────────────────────────────┘     └────────┬───────────┘
                                              │
                                              v
                                     ┌────────────────────┐
                                     │ Mirror Publisher    │
                                     │ (read + re-publish) │
                                     └────────┬───────────┘
                                              │ converts timestamp
                                              │ to epoch millis
                                              v
                                     ┌────────────────────┐     ┌──────────────┐
                                     │ Schema Topic       │────>│ Subscription │
                                     │ (Avro, JSON enc)   │     └──────┬───────┘
                                     │ v1 messages only   │            │
                                     └────────────────────┘            v
                                                              ┌───────────────┐
                                                              │ Dataflow      │
                                                              │ Pipeline      │
                                                              │ (schema-      │
                                                              │  driven)      │
                                                              └───────┬───────┘
                                                                      │ fetches Avro schema
                                                                      │ at startup
                                                                      v
                                                              ┌───────────────┐
                                                              │ BigQuery      │
                                                              │ (v1 rows)     │
                                                              └───────────────┘
```

### Phase 2: v1 + v2 Mix

```
                                     ┌────────────────────┐
                                ┌───>│ Sub A (no SMT)     │──> Mirror Publisher ──┐
                                │    │ (v1 messages)      │                      │
┌──────────────────────────┐    │    └────────────────────┘                      │
│ Public Taxi Topic        │────┤                                                │
│ (pubsub-public-data)     │    │    ┌────────────────────┐                      │
└──────────────────────────┘    └───>│ Sub B (with SMT)   │──> Mirror Publisher ──┤
                                     │ (v2 enriched)      │                      │
                                     └────────────────────┘                      │
                                                                                 │
                                              ┌──────────────────────────────────┘
                                              v
                                     ┌────────────────────┐     ┌──────────────┐
                                     │ Schema Topic       │────>│ Subscription │
                                     │ (Avro, JSON enc)   │     └──────┬───────┘
                                     │ v1 + v2 messages   │            │
                                     └────────────────────┘            v
                                                              ┌───────────────┐
                                                              │ Dataflow      │
                                                              │ Pipeline      │
                                                              │ (restarted,   │
                                                              │  same code)   │
                                                              └───────┬───────┘
                                                                      │ fetches v2 schema
                                                                      │ at startup
                                                                      v
                                                              ┌───────────────┐
                                                              │ BigQuery      │
                                                              │ (v1+v2 rows)  │
                                                              └───────────────┘
```

Both Sub A and Sub B receive all messages from the public topic. Sub B has a JavaScript UDF SMT
that enriches each message with additional fields. The mirror publisher code is identical for both
paths -- it reads from a subscription and re-publishes to the schema topic. You "turn on" v2 by
running a second publisher instance pointing at Sub B.

The pipeline code is identical between v1 and v2. The only difference is which schema revision
the registry returns at startup.

## Phase 1: Schema v1

### Avro Schema Definition (v1)

File: `schemas/taxi_ride_v1.avsc`

```json
{
  "type": "record",
  "name": "TaxiRide",
  "namespace": "com.example.taxi",
  "fields": [
    {"name": "ride_id", "type": "string"},
    {"name": "point_idx", "type": "int"},
    {"name": "latitude", "type": "double"},
    {"name": "longitude", "type": "double"},
    {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "meter_reading", "type": "double"},
    {"name": "meter_increment", "type": "double"},
    {"name": "ride_status", "type": "string"},
    {"name": "passenger_count", "type": "int"}
  ]
}
```

These 9 fields match the payload structure from the public taxi topic. The `timestamp` field
uses the Avro `timestamp-millis` logical type (a `long` representing epoch milliseconds),
which maps directly to BigQuery `TIMESTAMP`. The mirror publisher converts the source ISO 8601
string to epoch milliseconds before publishing. See [Avro Logical Types for Timestamps](#avro-logical-types-for-timestamps)
for rationale.

The `.avsc` file is only used as input to `gcloud pubsub schemas create`. After the schema
is registered, all consumers (the deployment script, the pipeline) fetch the schema definition
from the Pub/Sub Schema Registry, not from the local file.

### Avro-to-BigQuery Type Mapping

The pipeline dynamically maps Avro types to BigQuery types at startup. No hardcoded field
list -- the mapping function parses the Avro schema JSON and generates BQ schema fields.

| Avro Type | BQ Type | BQ Mode | Example Fields |
|---|---|---|---|
| `"string"` | STRING | REQUIRED | `ride_id`, `ride_status` |
| `"int"` | INT64 | REQUIRED | `point_idx`, `passenger_count` |
| `"long"` | INT64 | REQUIRED | |
| `"float"` | FLOAT64 | REQUIRED | |
| `"double"` | FLOAT64 | REQUIRED | `latitude`, `meter_reading` |
| `"boolean"` | BOOL | REQUIRED | |
| `{"type": "long", "logicalType": "timestamp-millis"}` | TIMESTAMP | REQUIRED | `timestamp` |
| `{"type": "long", "logicalType": "timestamp-micros"}` | TIMESTAMP | REQUIRED | |
| `{"type": "int", "logicalType": "date"}` | DATE | REQUIRED | |
| `["null", "string"]` | STRING | NULLABLE | `region` (v2) |
| `["null", "int"]` | INT64 | NULLABLE | |
| `["null", {"type": "long", "logicalType": "timestamp-millis"}]` | TIMESTAMP | NULLABLE | `enrichment_timestamp` (v2) |

The mapping function handles:
- Simple types (`"string"`, `"int"`, `"double"`, etc.)
- Avro logical types (`timestamp-millis`, `timestamp-micros`, `date`)
- Union types (`["null", T]`) mapped to NULLABLE mode

### Infrastructure Setup

The `run_dataflow_schema_driven.sh` script provisions the following resources:

1. **GCS bucket** -- Temp/staging location for Dataflow.
2. **BigQuery dataset** -- Target dataset.
3. **Pub/Sub Schema** -- Avro schema created from `schemas/taxi_ride_v1.avsc` (one-time upload).
4. **Schema-enabled Topic** -- Topic associated with the schema, JSON encoding.
5. **Pipeline Subscription** -- Subscription on the schema topic (for Dataflow to consume).
6. **Source Subscription (Sub A)** -- Subscription on the public taxi topic (for the mirror publisher).
7. **BigQuery table** -- Created dynamically. The script fetches the schema from the registry
   (not from the local `.avsc` file), maps Avro types to BQ types, and creates/updates the
   table with the generated schema plus envelope columns. Partitioned on `publish_time` (DAY).

No DLQ table is needed. The schema registry validates messages at publish time, so all
messages that reach the pipeline are guaranteed to conform to the schema.

On subsequent runs (e.g., after a v2 schema commit), the script re-fetches the schema and
updates the BQ table with `bq update --schema=...` to add any new columns. This is idempotent.

### Mirror Publisher

File: `publish_to_schema_topic.py`

A standalone Python script that:

1. Reads messages from the source subscription (public taxi topic) using `SubscriberClient`.
2. Parses the JSON payload.
3. Converts the `timestamp` field from ISO 8601 string to epoch milliseconds (to match
   the Avro `timestamp-millis` logical type). The value is published as a JSON number.
4. Re-publishes the converted JSON payload to the schema-enabled topic using `PublisherClient`.
5. Pub/Sub validates the message against the Avro schema at publish time.
6. Messages that fail schema validation are rejected by Pub/Sub with `INVALID_ARGUMENT`.

The publisher runs as a separate process (not part of Dataflow). It bridges the unschematized
public topic to the schema-governed topic.

CLI args:
- `--project` -- GCP project ID.
- `--source-subscription` -- Subscription to read from (on the public topic).
- `--target-topic` -- Schema-enabled topic to publish to.

### Schema-Driven Dataflow Pipeline

File: `dataflow_pubsub_to_bq/pipeline_schema_driven.py`

The pipeline fetches the Avro schema from the Pub/Sub Schema Registry at startup and uses it
to drive both the BigQuery table schema and the field extraction logic. No hardcoded field
lists anywhere.

**Startup flow:**

```
pipeline_schema_driven.py
    |
    ├── 1. Parse --pubsub_schema option (e.g., projects/.../schemas/taxi-ride-schema)
    ├── 2. Fetch schema definition via SchemaServiceClient.get_schema()
    ├── 3. Parse Avro JSON to extract field names and types
    ├── 4. Generate BQ schema: envelope fields (static) + payload fields (dynamic)
    ├── 5. Pass payload field names to ParseSchemaDrivenMessage DoFn
    └── 6. Pass full BQ schema to WriteToBigQuery
```

**Pipeline DAG:**

```
ReadFromPubSub (with_attributes=True)
    |
    v
ParseSchemaDrivenMessage (DoFn)
    |
    v
WriteToBigQuery (envelope + dynamic payload columns)
```

No DLQ path is needed. The schema registry validates messages at publish time, so every
message that reaches the pipeline is guaranteed to conform to the schema. This is a key
simplification compared to `pipeline.py`, which routes malformed messages to a DLQ table.

**The DoFn (`ParseSchemaDrivenMessage`):**

Receives the list of payload field names at construction time (from the parsed Avro schema).
At runtime, it:

1. Decodes message data and parses JSON.
2. Extracts static envelope fields (subscription metadata, schema governance attributes).
3. Dynamically extracts all payload fields using a loop over the schema field names:
   `for field_name in self.payload_field_names: bq_row[field_name] = ride_data.get(field_name)`

When the schema evolves from v1 (9 fields) to v2 (11 fields), the same code runs unchanged.
The DoFn receives `["ride_id", "point_idx", ..., "enrichment_timestamp", "region"]` instead
of `["ride_id", "point_idx", ...]` and automatically extracts the new fields. For v1 messages
missing the v2 fields, `.get()` returns `None` (stored as NULL in BigQuery).

**Pipeline options (`pipeline_schema_driven_options.py`):**

Extends the existing options with:
- `--pubsub_schema` -- Full resource path of the Pub/Sub schema
  (e.g., `projects/my-project/schemas/taxi-ride-schema`).

Reuses from `pipeline_options.py`:
- `--subscription`, `--output_table`, `--subscription_name`.

### BigQuery Table Schema

The table has two categories of columns:

**Envelope columns (static, always present -- 8 columns):**

| Column | Type | Source | Description |
|---|---|---|---|
| `subscription_name` | STRING | Pipeline config | Short name of the consuming subscription |
| `message_id` | STRING | Pub/Sub metadata | Unique message identifier |
| `publish_time` | TIMESTAMP | Pub/Sub metadata | When the message was published |
| `processing_time` | TIMESTAMP | Wall clock | When the Dataflow worker processed it |
| `attributes` | STRING | Pub/Sub metadata | JSON-serialized message attributes |
| `schema_name` | STRING | `googclient_schemaname` | Schema used for validation |
| `schema_revision_id` | STRING | `googclient_schemarevisionid` | Revision that validated the message |
| `schema_encoding` | STRING | `googclient_schemaencoding` | Encoding type (JSON or BINARY) |

**Payload columns (dynamic, from Avro schema -- 9 columns for v1, 11 for v2):**

v1:

| Column | Type | Source |
|---|---|---|
| `ride_id` | STRING | Avro `string` |
| `point_idx` | INT64 | Avro `int` |
| `latitude` | FLOAT64 | Avro `double` |
| `longitude` | FLOAT64 | Avro `double` |
| `timestamp` | TIMESTAMP | Avro `timestamp-millis` |
| `meter_reading` | FLOAT64 | Avro `double` |
| `meter_increment` | FLOAT64 | Avro `double` |
| `ride_status` | STRING | Avro `string` |
| `passenger_count` | INT64 | Avro `int` |

v2 adds:

| Column | Type | Source |
|---|---|---|
| `enrichment_timestamp` | TIMESTAMP | Avro `["null", timestamp-millis]` |
| `region` | STRING | Avro `["null", "string"]` |

Table is partitioned on `publish_time` (DAY) and clustered on `publish_time`.

### Files to Create

| File | Purpose |
|---|---|
| `schemas/taxi_ride_v1.avsc` | Avro schema definition (input for registry upload only) |
| `dataflow_pubsub_to_bq/pipeline_schema_driven.py` | Pipeline entry point: fetches schema from registry, dynamic BQ schema + extraction |
| `dataflow_pubsub_to_bq/pipeline_schema_driven_options.py` | Pipeline options: adds `--pubsub_schema` |
| `dataflow_pubsub_to_bq/transforms/schema_driven_to_tablerow.py` | DoFn + Avro-to-BQ type mapper + envelope schema |
| `publish_to_schema_topic.py` | Mirror publisher (timestamp conversion + re-publish) |
| `run_dataflow_schema_driven.sh` | End-to-end deployment script (fetches schema from registry for BQ table) |
| `tests/test_schema_driven_to_tablerow.py` | Unit tests for dynamic extraction and type mapping |

## Phase 2: Schema v2 Evolution (Future)

Phase 2 is executed after the v1 pipeline is proven and running. The goal is to demonstrate
that the schema-driven pipeline handles both v1 and v2 messages after a schema revision and
a Dataflow job restart -- with **zero pipeline code changes**.

### Avro Schema Definition (v2)

File: `schemas/taxi_ride_v2.avsc`

```json
{
  "type": "record",
  "name": "TaxiRide",
  "namespace": "com.example.taxi",
  "fields": [
    {"name": "ride_id", "type": "string"},
    {"name": "point_idx", "type": "int"},
    {"name": "latitude", "type": "double"},
    {"name": "longitude", "type": "double"},
    {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "meter_reading", "type": "double"},
    {"name": "meter_increment", "type": "double"},
    {"name": "ride_status", "type": "string"},
    {"name": "passenger_count", "type": "int"},
    {"name": "enrichment_timestamp", "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}], "default": null},
    {"name": "region", "type": ["null", "string"], "default": null}
  ]
}
```

The v2 schema adds two optional fields with `null` defaults:

- `enrichment_timestamp`: Timestamp when the SMT enriched the message. Uses `timestamp-millis`
  logical type (nullable) to map to BQ `TIMESTAMP`.
- `region`: Derived field (e.g., based on latitude/longitude ranges). Nullable string.

Avro schema resolution rules guarantee backward/forward compatibility when adding optional
fields with defaults. A v1 message (missing these fields) is valid against v2 because the
defaults apply. A v2 message (with these fields) is valid against v1 because unknown fields
are ignored during resolution.

### Subscription SMT for Enrichment

A JavaScript UDF SMT is attached to Subscription B on the public taxi topic. The SMT
enriches each message before the subscriber receives it.

```javascript
function enrichTaxiRide(message, metadata) {
  const data = JSON.parse(message.data);

  // Add enrichment timestamp (epoch millis for timestamp-millis logical type)
  data.enrichment_timestamp = Date.now();

  // Derive region from latitude (simplified example)
  if (data.latitude > 40.8) {
    data.region = "north";
  } else if (data.latitude > 40.75) {
    data.region = "midtown";
  } else {
    data.region = "downtown";
  }

  message.data = JSON.stringify(data);
  return message;
}
```

The SMT runs inside Pub/Sub -- no additional compute resources needed. The enriched message
is delivered to the subscriber (mirror publisher), which re-publishes it to the schema topic.
The schema topic validates the enriched message against the v2 revision.

### Steps to Evolve

Execute these steps in order after v1 is proven. The ordering is critical -- see
[Ordering Dependency](#ordering-dependency) for why steps 1-2 must complete before step 4.

#### Step 0: Demonstrate Schema Rejection (Governance Proof)

Before evolving the schema, prove that the schema registry blocks non-conforming messages.
Create Sub B with SMT and attempt to publish a v2 message while the schema is still v1-only.

```bash
# Create Sub B with SMT (enriches messages with v2 fields)
gcloud pubsub subscriptions create taxi_telemetry_schema_v2_source \
    --project=${PROJECT_ID} \
    --topic=projects/pubsub-public-data/topics/taxirides-realtime \
    --message-transform='javascriptUdf={code="function enrichTaxiRide(message, metadata) { const data = JSON.parse(message.data); data.enrichment_timestamp = Date.now(); data.region = \"test\"; message.data = JSON.stringify(data); return message; }",functionName="enrichTaxiRide"}'

# Attempt to publish a v2 message (schema is still v1-only)
python publish_to_schema_topic.py \
    --project=${PROJECT_ID} \
    --source-subscription=taxi_telemetry_schema_v2_source \
    --target-topic=${SCHEMA_TOPIC_NAME}
```

**Expected result:** The mirror publisher logs `INVALID_ARGUMENT` errors from Pub/Sub.
The v2 messages are rejected because the topic only accepts v1 schema. No v2 data enters
the system. This proves schema governance enforcement is working.

#### Step 1: Commit v2 Schema Revision

```bash
gcloud pubsub schemas commit taxi-ride-schema \
    --type=avro \
    --definition-file=schemas/taxi_ride_v2.avsc
```

This creates a new revision under the existing schema. Note the revision ID from the output.
The schema now has two revisions (v1 and v2), but the topic is not yet configured to accept v2.

#### Step 2: Update Topic Revision Range

```bash
gcloud pubsub topics update ${SCHEMA_TOPIC_NAME} \
    --schema=taxi-ride-schema \
    --first-revision-id=${V1_REVISION_ID} \
    --last-revision-id=${V2_REVISION_ID} \
    --message-encoding=JSON
```

The topic now accepts messages matching ANY revision between v1 and v2 (inclusive).
v1 messages (9 fields) are validated against v1. v2 messages (11 fields) are validated
against v2.

#### Step 3: Update BigQuery Table and Restart Pipeline

Re-run the deployment script. It re-fetches the schema (now v2) from the registry,
generates the updated BQ schema (with the 2 new columns), updates the table via
`bq update --schema=...`, and submits a new Dataflow job.

```bash
# Drain the current job first
gcloud dataflow jobs drain ${CURRENT_JOB_ID} --region=${REGION}

# Re-run the same deployment script -- no code changes needed
./run_dataflow_schema_driven.sh
```

The deployment script:
1. Fetches v2 schema from registry (now has 11 fields).
2. Updates BQ table schema -- adds `enrichment_timestamp` and `region` columns.
   Existing rows get NULL for these columns. This is non-breaking.
3. Submits the pipeline. The pipeline fetches the v2 schema at startup, generates
   the updated BQ schema and field list, and begins processing.

**No pipeline code changes.** The same `pipeline_schema_driven.py` runs with 11 fields
instead of 9, purely driven by the schema registry content.

#### Step 4: Start v2 Mirror Publisher

Run a second instance of the mirror publisher pointing at Subscription B (created in Step 0):

```bash
python publish_to_schema_topic.py \
    --project=${PROJECT_ID} \
    --source-subscription=taxi_telemetry_schema_v2_source \
    --target-topic=${SCHEMA_TOPIC_NAME}
```

**Expected result:** v2 messages now pass validation (because Steps 1-2 expanded the
revision range). The schema topic receives a mix of v1 (from Sub A publisher) and v2
(from Sub B publisher) messages. The pipeline processes both -- v1 rows have NULL for
the new columns, v2 rows have populated values.

### Demo Narrative

The Phase 2 demonstration tells a clear governance story in four acts:

1. **Rejection proof (Step 0):** Show that the schema registry blocks v2 messages when only
   v1 is configured. The mirror publisher gets `INVALID_ARGUMENT` errors. This proves that
   schema evolution is a deliberate process, not something that can happen accidentally.

2. **Schema evolution (Steps 1-2):** Commit the v2 revision and expand the topic's revision
   range. This is the "governance approval" step -- the team explicitly declares that v2
   is now an accepted format.

3. **Zero-code restart (Step 3):** Re-run the same deployment script. The pipeline
   automatically picks up the v2 schema, generates the updated BQ schema, and processes
   v2 fields. No `.py` files were edited. The schema registry is the single source of truth.

4. **Mixed traffic (Step 4):** Start the v2 mirror publisher. Both v1 and v2 messages
   coexist on the schema topic. The pipeline processes both. Query BQ to see mixed rows
   with `schema_revision_id` distinguishing v1 from v2, and v2 rows having populated
   `enrichment_timestamp` and `region` columns while v1 rows have NULLs.

### Validation Queries

After the pipeline processes mixed messages, run these queries to verify:

```sql
-- Count messages by schema revision
SELECT
  schema_revision_id,
  COUNT(*) as message_count,
  MIN(publish_time) as first_seen,
  MAX(publish_time) as last_seen
FROM `${PROJECT_ID}.${DATASET}.${TABLE}`
GROUP BY schema_revision_id
ORDER BY first_seen;
```

```sql
-- Show v2 enrichment fields are populated, v1 has NULLs
SELECT
  schema_revision_id,
  ride_id,
  enrichment_timestamp,
  region,
  CASE WHEN enrichment_timestamp IS NOT NULL THEN 'v2' ELSE 'v1' END as schema_version
FROM `${PROJECT_ID}.${DATASET}.${TABLE}`
ORDER BY publish_time DESC
LIMIT 20;
```

```sql
-- Verify no data loss during transition
SELECT
  DATE(publish_time) as day,
  schema_revision_id,
  COUNT(*) as count
FROM `${PROJECT_ID}.${DATASET}.${TABLE}`
GROUP BY day, schema_revision_id
ORDER BY day DESC, schema_revision_id;
```

## Schema Governance Enforcement

This section explains where schema validation happens in the architecture and what occurs
when a message does not conform to the schema -- a critical detail for understanding the
ordering dependency in Phase 2.

### Where Validation Lives

Schema validation is associated with the **topic**, not the subscription. Pub/Sub validates
messages when they are **published to the schema-enabled topic**. Subscriptions on the topic
(where Dataflow reads) do not perform schema validation -- they simply deliver the already-
validated messages.

```
Sub B (on public topic, with SMT)     <-- No schema validation here.
    |                                     This is just a subscription with a
    v                                     JavaScript UDF transform.
Mirror Publisher reads enriched msg   <-- No schema validation here.
    |                                     The publisher is a plain Python process.
    v
publisher.publish(schema_topic, data) <-- SCHEMA VALIDATION HAPPENS HERE.
    |                                     The topic checks: does this JSON match
    |                                     any Avro revision in my allowed range?
    v
Schema Topic stores message           <-- Message is stored with googclient_*
    |                                     attributes stamped by Pub/Sub.
    v
Dataflow reads from subscription      <-- No schema validation here.
                                          The pipeline trusts the message because
                                          it was validated at publish time.
```

This means:

- The source subscriptions (Sub A, Sub B on the public topic) have nothing to do with
  schema validation. They are just delivery mechanisms.
- The Subscription SMT on Sub B transforms messages before the subscriber reads them,
  but the SMT itself does not validate against any schema.
- The schema-enabled topic is the single enforcement point. It accepts or rejects messages
  based on its configured schema and revision range.

### What Happens When v2 Data Hits a v1 Schema

If the schema registry only has a v1 revision and you attempt to publish a v2 message
(with extra fields like `enrichment_timestamp` and `region`), **the publish is rejected**.
Pub/Sub returns `INVALID_ARGUMENT`.

The validation flow:

1. Mirror publisher (from Sub B with SMT) calls `publish()` with an enriched JSON payload
   containing 11 fields (9 original + 2 new).
2. The schema topic checks its allowed revision range -- only v1 is configured.
3. Pub/Sub validates the JSON against the v1 Avro schema (9 fields).
4. The v2 message has fields (`enrichment_timestamp`, `region`) not defined in v1.
5. Validation fails. Pub/Sub returns `INVALID_ARGUMENT`. The message is never stored.

This is the intended behavior. The schema registry enforces the contract: you cannot
publish data in a format that no known schema revision recognizes. This prevents
accidental schema drift and guarantees that every message in the topic conforms to a
declared schema version.

### Ordering Dependency

Because the topic rejects messages that don't match any configured revision, the
Phase 2 evolution steps have a **strict ordering dependency**:

```
Step 1: Commit v2 schema revision         <-- Schema now has v1 AND v2 definitions
Step 2: Update topic revision range        <-- Topic now ACCEPTS both v1 and v2
   ─── GATE: Steps 1-2 must complete before step 4 ───
Step 3: Re-run deployment script           <-- Updates BQ table + restarts pipeline
                                               (zero code changes)
Step 4: Start v2 mirror publisher          <-- v2 messages now pass validation
```

If you attempt Step 4 before Steps 1-2, every v2 message is rejected. The mirror publisher
would see continuous `INVALID_ARGUMENT` errors from Pub/Sub and no v2 messages would reach
the schema topic.

This is a deliberate governance mechanism: schema evolution is an explicit, controlled
process. You must declare the new format (commit revision) and authorize it on the topic
(update revision range) before any data in that format can enter the system.

## Key Design Decisions

### Why Schema-Driven (Not Hardcoded)

The existing `pipeline.py` uses hardcoded `.get()` calls and a hardcoded BQ schema function.
Adding a new field requires editing the transform, the schema function, the run script, and
the BQ table. This pipeline variant eliminates all of that.

| Aspect | `pipeline.py` (hardcoded) | `pipeline_schema_driven.py` |
|---|---|---|
| Field extraction | Hardcoded `.get()` calls per field | Dynamic loop over schema field names |
| BQ schema | Hardcoded `get_bigquery_schema()` | Generated from Avro at startup |
| v2 code changes | Edit transform + schema function | None -- re-run same script |
| Source of truth | Python code | Pub/Sub Schema Registry |
| BQ table update | Manual `ALTER TABLE` | Script auto-updates via `bq update` |
| Demo value | Simple Pub/Sub consumption | Zero-code schema evolution |

Both pipelines coexist in the repo as different demo use cases:
- `pipeline.py` + `run_dataflow.sh`: Simple Pub/Sub to BQ with manual parsing.
- `pipeline_schema_driven.py` + `run_dataflow_schema_driven.sh`: Schema registry with
  automatic adaptation.

### Why Avro Over Protocol Buffers

Avro was chosen for schema evolution flexibility:

- Avro allows adding optional fields with defaults and maintains full backward/forward
  compatibility. A consumer with v2 schema can read v1 messages (defaults fill missing
  fields), and a consumer with v1 schema can read v2 messages (unknown fields are ignored).
- Protocol Buffer revisions are more restrictive in Pub/Sub: you can only add or remove
  `optional` fields. You cannot add or delete required fields, and you cannot edit existing
  fields.
- Avro's schema resolution rules are well-documented and explicitly referenced in the
  [Pub/Sub docs](https://cloud.google.com/pubsub/docs/commit-schema-revision) for handling
  revision compatibility.

### Why JSON Encoding (Not Binary)

The topic is configured with `--message-encoding=JSON`. This means messages are published
and delivered as plain JSON strings. The Avro schema validates the JSON structure at publish
time, but the message payload is never converted to Avro binary format.

**Implications:**

- The pipeline uses `json.loads()` to parse messages -- same approach as existing pipelines.
- No Avro serialization/deserialization libraries needed in the pipeline code.
- Messages are human-readable in logs and debugging tools.
- Wire size is larger than binary encoding (~30-50% overhead), but acceptable for this
  use case.

**When binary encoding is preferred:**

- High-throughput systems where wire size matters.
- Avro-native ecosystems (Confluent Schema Registry, Spark, Flink).
- Using the Dataflow Avro template (`PubsubIO.readAvroGenericRecords()`).
- BigQuery subscription with "Use topic schema" for zero-code ingestion.

### Schema Registry as Validation vs Encoding

With JSON encoding, the Avro schema registry serves as a **validation and governance layer**,
not an encoding layer. The message payload stays as JSON end-to-end.

This is a valid and common use case. The comparison with in-pipeline validation
(`json_to_tablerow.py`) highlights where the value lies:

| Aspect | In-Pipeline Validation | Schema Registry (JSON enc) |
|---|---|---|
| When validation runs | Processing time (in Dataflow) | Publish time (at Pub/Sub ingress) |
| Where validation runs | Consumer side | Producer side |
| Bad data behavior | Enters system, routed to DLQ | Rejected at publish time, never stored (no DLQ needed) |
| Schema versioning | None (implicit in code) | Explicit revisions with unique IDs |
| Cross-team contract | Code is the contract | Schema is the contract |
| Evolution management | Code changes + deploys | `gcloud pubsub schemas commit` |
| Per-message tracking | Not available | `googclient_schemarevisionid` attribute |

The "full Avro" pattern (JSON input -> Avro binary encode -> publish -> Avro binary decode)
exists for systems that need compact wire format and Avro-native integration. For this
demonstration, JSON encoding cleanly separates the governance concern from the encoding
concern.

### Avro Logical Types for Timestamps

Avro supports timestamps via logical types that annotate a `long` base type:

- `timestamp-millis`: epoch milliseconds (maps to BQ `TIMESTAMP`).
- `timestamp-micros`: epoch microseconds (maps to BQ `TIMESTAMP`).

The public taxi data sends timestamps as ISO 8601 strings (e.g.,
`"2026-01-15T05:45:49.16882-05:00"`). Two approaches exist:

**Option A: Avro `string` type, BQ `STRING` column.**
Simpler -- no conversion needed. But loses BQ TIMESTAMP benefits (partitioning, time-range
queries, native timestamp functions).

**Option B: Avro `timestamp-millis` logical type, BQ `TIMESTAMP` column (chosen).**
The mirror publisher converts ISO 8601 strings to epoch milliseconds before publishing.
This is a one-time conversion in the publisher, and keeps the schema self-describing --
the Avro schema declares "this field is a timestamp", and the pipeline's type mapper
automatically generates a BQ `TIMESTAMP` column. No special-case logic in the pipeline.

The Pub/Sub docs confirm this mapping. From the
[BigQuery subscription schema compatibility](https://cloud.google.com/pubsub/docs/bigquery):
`timestamp-millis` and `timestamp-micros` Avro logical types map to BQ `TIMESTAMP`.

## Pub/Sub Schema Registry Concepts

### Schema Revisions

A Pub/Sub schema can have multiple revisions. Each revision:

- Has a unique auto-generated 8-character revision ID.
- Contains a complete schema definition (not a diff).
- Must be compatible with other revisions per the schema type rules:
  - **Avro:** Follow [Avro schema resolution](https://avro.apache.org/docs/++version++/specification/#schema-resolution)
    rules. A new revision must work as both reader and writer schema.
  - **Protobuf:** Can only add or remove `optional` fields.
- Maximum of 20 revisions per schema.

### Publish-Time Validation

When a topic is associated with a schema:

1. Publisher sends a message to the topic.
2. If a Topic SMT is configured, it transforms the message first.
3. Pub/Sub validates the message against the schema revisions in the topic's allowed range.
4. Validation checks revisions from newest to oldest until a match is found.
5. If no revision matches, Pub/Sub returns `INVALID_ARGUMENT` and the publish fails.
6. On success, Pub/Sub adds `googclient_*` attributes to the message.

### Schema Metadata Attributes

Pub/Sub automatically adds these attributes to every message validated against a schema:

| Attribute | Description | Example |
|---|---|---|
| `googclient_schemaname` | Full resource name of the schema | `projects/my-project/schemas/taxi-ride-schema` |
| `googclient_schemaencoding` | Encoding type | `JSON` or `BINARY` |
| `googclient_schemarevisionid` | Revision ID that validated the message | `abcd1234` |

These attributes are immutable after publish. Committing a new schema revision or changing
the topic's schema association does not retroactively modify existing messages.

### Single Message Transforms (SMTs)

Pub/Sub SMTs are lightweight JavaScript UDF transforms that modify messages inline, without
external compute resources. Key properties:

- Up to 5 SMTs per topic or subscription.
- SMTs operate on a single message (no aggregation).
- Input/output: message `data` (string) and `attributes` (map of string to string).
- Return `null` to filter (drop) a message.
- Topic SMTs run before schema validation; subscription SMTs run before delivery to
  subscribers.

**Topic SMT vs Subscription SMT:**

| Aspect | Topic SMT | Subscription SMT |
|---|---|---|
| When it runs | Before schema validation, before storage | Before delivery to subscriber |
| Affects | All subscriptions on the topic | Only the specific subscription |
| Use case | Data enrichment at ingress | Per-consumer data masking, filtering |

In the Phase 2 architecture, a **Subscription SMT** on the source subscription (public taxi
topic) enriches messages before the mirror publisher reads them. This is preferred over a
Topic SMT because:

- The source topic (public data) cannot be modified.
- The enrichment only applies to the v2 publisher path, not the v1 path.
- Subscription SMTs allow independent control of v1 vs v2 message flows.

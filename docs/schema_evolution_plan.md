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
  - [File Inventory](#file-inventory)
- [Phase 2: Schema v2 Evolution](#phase-2-schema-v2-evolution)
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
  - [Why Schema-Driven (Not Hardcoded or Raw JSON)](#why-schema-driven-not-hardcoded-or-raw-json)
  - [Why Avro Over Protocol Buffers](#why-avro-over-protocol-buffers)
  - [Why JSON Encoding (Not Binary)](#why-json-encoding-not-binary)
  - [Schema Registry as Validation vs Encoding](#schema-registry-as-validation-vs-encoding)
  - [Custom Logical Type for Timestamps](#custom-logical-type-for-timestamps)
- [Pub/Sub Schema Registry Concepts](#pubsub-schema-registry-concepts)
  - [Schema Revisions](#schema-revisions)
  - [Publish-Time Validation](#publish-time-validation)
  - [Schema Metadata Attributes](#schema-metadata-attributes)
  - [Single Message Transforms (SMTs)](#single-message-transforms-smts)

## Overview

This repository demonstrates three streaming ingestion patterns (see [README](../README.md)):

1. **Standard ETL** (`pipeline.py`) -- parses JSON fields into typed BQ columns. Simple but
   schema changes require code changes and redeployment.
2. **Raw JSON ELT** (`pipeline_json.py`) -- ingests raw payload into a BQ `JSON` column.
   Recommended for most use cases because it decouples ingestion from transformation and
   eliminates data loss risk from schema changes.
3. **Schema-Driven ETL** (`pipeline_schema_driven.py`) -- this document's focus.

The schema-driven pipeline is the right choice when you need **schema governance** (publish-time
validation, explicit versioning, cross-team contracts) but still want typed BigQuery columns
rather than raw JSON. If governance is not a requirement, the Raw JSON ELT pattern is simpler
and more resilient to schema changes.

This pipeline variant introduces **Pub/Sub Schema Registry** with a **schema-driven** architecture:

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
                                      │ (pure pass-through) │
                                      └────────┬───────────┘
                                               │
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
    {"name": "timestamp", "type": {"type": "string", "logicalType": "iso-datetime"}},
    {"name": "meter_reading", "type": "double"},
    {"name": "meter_increment", "type": "double"},
    {"name": "ride_status", "type": "string"},
    {"name": "passenger_count", "type": "int"}
  ]
}
```

These 9 fields match the payload structure from the public taxi topic. The `timestamp` field
uses a **custom Avro logical type** `iso-datetime` on a `string` base type. This signals to
the pipeline code that the string contains an ISO 8601 timestamp and should be converted to
a BigQuery `TIMESTAMP` column. The mirror publisher passes the timestamp string through
as-is -- no conversion needed. See [Custom Logical Type for Timestamps](#custom-logical-type-for-timestamps)
for rationale.

The `.avsc` file is only used as input to `gcloud pubsub schemas create`. After the schema
is registered, all consumers (the deployment script, the pipeline) fetch the schema definition
from the Pub/Sub Schema Registry, not from the local file.

### Avro-to-BigQuery Type Mapping

The pipeline dynamically maps Avro types to BigQuery types at startup. No hardcoded field
list -- the mapping function parses the Avro schema JSON and generates BQ schema fields.

| Avro Type | BQ Type | BQ Mode | Example Fields |
|---|---|---|---|
| `"string"` | STRING | NULLABLE | `ride_id`, `ride_status` |
| `"int"` | INT64 | NULLABLE | `point_idx`, `passenger_count` |
| `"long"` | INT64 | NULLABLE | |
| `"float"` | FLOAT64 | NULLABLE | |
| `"double"` | FLOAT64 | NULLABLE | `latitude`, `meter_reading` |
| `"boolean"` | BOOL | NULLABLE | |
| `{"type": "string", "logicalType": "iso-datetime"}` | TIMESTAMP | NULLABLE | `timestamp` |
| `{"type": "long", "logicalType": "timestamp-millis"}` | TIMESTAMP | NULLABLE | |
| `{"type": "long", "logicalType": "timestamp-micros"}` | TIMESTAMP | NULLABLE | |
| `{"type": "int", "logicalType": "date"}` | DATE | NULLABLE | |
| `["null", "string"]` | STRING | NULLABLE | `region` (v2) |
| `["null", "int"]` | INT64 | NULLABLE | |
| `["null", {"type": "string", "logicalType": "iso-datetime"}]` | TIMESTAMP | NULLABLE | `enrichment_timestamp` (v2) |

The mapping function handles:
- Simple types (`"string"`, `"int"`, `"double"`, etc.)
- Standard Avro logical types (`timestamp-millis`, `timestamp-micros`, `date`)
- Custom logical types (`iso-datetime` for ISO 8601 timestamp strings)
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

File: `scripts/publish_to_schema_topic.py`

A standalone Python script that acts as a **pure pass-through relay**:

1. Reads messages from the source subscription (public taxi topic) using `SubscriberClient`.
2. Re-publishes the JSON payload as-is to the schema-enabled topic using `PublisherClient`.
3. Pub/Sub validates the message against the Avro schema at publish time.
4. Messages that fail schema validation are rejected by Pub/Sub with `INVALID_ARGUMENT`.

The publisher performs **no data transformation**. The `timestamp` field stays as an ISO 8601
string, which matches the Avro schema's `string` base type (with `iso-datetime` custom logical
type annotation). All type conversion (e.g., ISO 8601 string to BQ TIMESTAMP) happens in the
Dataflow pipeline's DoFn, where business logic belongs.

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
   `for field_name in self.payload_field_names: bq_row[field_name] = payload.get(field_name)`

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
| `timestamp` | TIMESTAMP | Avro `string` with `iso-datetime` logical type |
| `meter_reading` | FLOAT64 | Avro `double` |
| `meter_increment` | FLOAT64 | Avro `double` |
| `ride_status` | STRING | Avro `string` |
| `passenger_count` | INT64 | Avro `int` |

v2 adds:

| Column | Type | Source |
|---|---|---|
| `enrichment_timestamp` | TIMESTAMP | Avro `["null", iso-datetime]` |
| `region` | STRING | Avro `["null", "string"]` |

Table is partitioned on `publish_time` (DAY) and clustered on `publish_time`.

### File Inventory

| File | Phase | Purpose |
|---|---|---|
| `schemas/taxi_ride_v1.avsc` | v1 | Avro schema definition (input for registry upload only) |
| `schemas/taxi_ride_v2.avsc` | v2 | Avro v2 schema with `enrichment_timestamp` and `region` |
| `dataflow_pubsub_to_bq/pipeline_schema_driven.py` | v1 | Pipeline entry point: fetches schema from registry, dynamic BQ schema + extraction |
| `dataflow_pubsub_to_bq/pipeline_schema_driven_options.py` | v1 | Pipeline options: adds `--pubsub_schema` |
| `dataflow_pubsub_to_bq/transforms/schema_driven_to_tablerow.py` | v1 | DoFn + Avro-to-BQ type mapper + envelope schema |
| `scripts/publish_to_schema_topic.py` | v1 | Mirror publisher (pure pass-through relay, used by both v1 and v2) |
| `scripts/generate_bq_schema.py` | v1 | BQ schema generator from Pub/Sub Schema Registry |
| `scripts/run_mirror_publisher.sh` | v1 | v1 mirror publisher launcher script |
| `scripts/enrich_taxi_ride.yaml` | v2 | SMT definition (JavaScript UDF) for v2 enrichment |
| `scripts/run_schema_evolution.sh` | v2 | Phase 2 orchestration: schema evolution + v2 mirror publisher |
| `run_dataflow_schema_driven.sh` | v1 | End-to-end deployment script (auto-cancels old job, fetches schema from registry) |
| `scripts/cleanup_schema_driven.sh` | v1+v2 | Tears down all schema-driven resources for a fresh start |
| `tests/test_schema_driven_to_tablerow.py` | v1+v2 | Unit tests for dynamic extraction and type mapping |

## Phase 2: Schema v2 Evolution

Phase 2 is executed after the v1 pipeline is proven and running. The entire Phase 2 workflow
is automated in `scripts/run_schema_evolution.sh`. The goal is to demonstrate that the
schema-driven pipeline handles both v1 and v2 messages after a schema revision and a
Dataflow job restart -- with **zero pipeline code changes**.

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
    {"name": "timestamp", "type": {"type": "string", "logicalType": "iso-datetime"}},
    {"name": "meter_reading", "type": "double"},
    {"name": "meter_increment", "type": "double"},
    {"name": "ride_status", "type": "string"},
    {"name": "passenger_count", "type": "int"},
    {"name": "enrichment_timestamp", "type": ["null", {"type": "string", "logicalType": "iso-datetime"}], "default": null},
    {"name": "region", "type": ["null", "string"], "default": null}
  ]
}
```

The v2 schema adds two optional fields with `null` defaults:

- `enrichment_timestamp`: Timestamp when the SMT enriched the message. Uses the custom
  `iso-datetime` logical type (nullable) to map to BQ `TIMESTAMP`.
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

  // Add enrichment timestamp (ISO 8601 string for iso-datetime logical type)
  data.enrichment_timestamp = new Date().toISOString();

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
[Ordering Dependency](#ordering-dependency) for why steps 1-3 must complete before step 4.

#### Step 0: Demonstrate Schema Enforcement (Governance Proof)

Before evolving the schema, demonstrate what the schema registry enforces. The SMT is
defined in a YAML file (`scripts/enrich_taxi_ride.yaml`) and passed to the gcloud CLI
via `--message-transforms-file`. The subscription creation implicitly validates the SMT.

The governance proof uses `gcloud pubsub schemas validate-message` to test four scenarios
against the v1 schema:

```bash
# 2a. Valid v1 message -- passes
gcloud pubsub schemas validate-message \
    --schema-name=projects/${PROJECT_ID}/schemas/${SCHEMA_NAME} \
    --message-encoding=json \
    --message='{"ride_id":"test","point_idx":1,...,"passenger_count":2}'

# 2b. Missing required field (passenger_count) -- INVALID_ARGUMENT
gcloud pubsub schemas validate-message \
    --schema-name=projects/${PROJECT_ID}/schemas/${SCHEMA_NAME} \
    --message-encoding=json \
    --message='{"ride_id":"test","point_idx":1,...}'

# 2c. Wrong type (point_idx as string) -- INVALID_ARGUMENT
gcloud pubsub schemas validate-message \
    --schema-name=projects/${PROJECT_ID}/schemas/${SCHEMA_NAME} \
    --message-encoding=json \
    --message='{"ride_id":"test","point_idx":"NOT_AN_INT",...,"passenger_count":2}'

# 2d. Extra fields (v2 message) -- passes (Avro forward compatibility)
gcloud pubsub schemas validate-message \
    --schema-name=projects/${PROJECT_ID}/schemas/${SCHEMA_NAME} \
    --message-encoding=json \
    --message='{"ride_id":"test","point_idx":1,...,"passenger_count":2,"enrichment_timestamp":"...","region":"midtown"}'
```

All of these steps are automated in `scripts/run_schema_evolution.sh`.

**Results:** The schema registry enforces **structural contracts**:

- **Required fields** must be present (2b is rejected with `INVALID_ARGUMENT`).
- **Field types** must be correct (2c is rejected with `INVALID_ARGUMENT`).
- **Extra fields** are allowed per Avro's forward compatibility rules (2d passes).

This means v2 messages (with extra fields like `enrichment_timestamp` and `region`) can
be published to a v1-only topic without errors. The extra fields are stored in Pub/Sub
but ignored by the v1 pipeline (it only extracts 9 fields). After upgrading to v2 and
restarting, the pipeline extracts all 11 fields.

#### Step 1: Commit v2 Schema Revision

```bash
gcloud pubsub schemas commit taxi-ride-schema \
    --type=avro \
    --definition-file=schemas/taxi_ride_v2.avsc
```

This creates a new revision under the existing schema. Note the revision ID from the output.
The schema now has two revisions (v1 and v2), but the topic is not yet configured to validate
against v2 (v2 messages are accepted via forward compatibility but stamped with the v1 revision ID).

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

Re-run the deployment script. It auto-cancels the running job, re-fetches the schema
(now v2) from the registry, generates the updated BQ schema (with the 2 new columns),
updates the table via `bq update --schema=...`, and submits a new Dataflow job.

```bash
# Re-run the same deployment script -- no code changes needed.
# The script auto-cancels any running schema-driven job before submitting.
./run_dataflow_schema_driven.sh
```

**Why cancel instead of drain:** Cancel is used for faster restarts. This is safe because
Pub/Sub retains unacknowledged messages in the subscription -- the new job picks up from
where the old one left off. The Storage Write API provides exactly-once semantics, so any
duplicate processing during the transition is handled automatically. Drain can take minutes
to hours for streaming pipelines (it waits for all in-flight data to flush), while cancel
is near-instant.

The deployment script:
1. Auto-cancels the running Dataflow job (waits for cancellation to complete).
2. Fetches v2 schema from registry (now has 11 fields).
3. Updates BQ table schema -- adds `enrichment_timestamp` and `region` columns.
   Existing rows get NULL for these columns. This is non-breaking.
4. Submits the pipeline. The pipeline fetches the v2 schema at startup, generates
   the updated BQ schema and field list, and begins processing.

**No pipeline code changes.** The same `pipeline_schema_driven.py` runs with 11 fields
instead of 9, purely driven by the schema registry content.

#### Step 4: Start v2 Mirror Publisher

Run the v2 mirror publisher using `scripts/run_schema_evolution.sh` (which handles this
as the final step), or manually:

```bash
uv run python scripts/publish_to_schema_topic.py \
    --project=${PROJECT_ID} \
    --source-subscription=taxi_telemetry_schema_v2_source \
    --target-topic=${SCHEMA_TOPIC_NAME}
```

**Expected result:** v2 messages now have their fields fully extracted (because Steps 1-3
ensure correct revision tracking and field extraction). The schema topic receives a mix of v1 (from Sub A publisher) and v2
(from Sub B publisher) messages. The pipeline processes both -- v1 rows have NULL for
the new columns, v2 rows have populated values.

### Demo Narrative

The Phase 2 demonstration tells a clear governance story in four acts:

1. **Enforcement proof (Step 0):** Demonstrate what the schema registry enforces using
   `gcloud pubsub schemas validate-message`. Show that missing required fields and wrong
   types are rejected (`INVALID_ARGUMENT`), while extra fields (v2 fields on a v1 schema)
   are accepted per Avro forward compatibility. This proves the registry enforces structural
   contracts -- required fields and correct types -- not field-level lockdown.

2. **Schema evolution (Steps 1-2):** Commit the v2 revision and expand the topic's revision
   range. This is the "governance approval" step -- the team explicitly declares that v2
   is now an accepted format.

3. **Zero-code restart (Step 3):** Re-run the same deployment script. The pipeline
   automatically picks up the v2 schema, generates the updated BQ schema, and processes
   v2 fields. No `.py` files were edited. The schema registry is the single source of truth.

4. **Mixed traffic (Step 4):** Start the v2 mirror publisher. Both v1 and v2 messages
   coexist on the schema topic. The pipeline processes both. Query BQ to see mixed rows
   using `enrichment_timestamp IS NOT NULL` to distinguish v2 (enriched) from v1 (plain)
   messages. v2 rows have populated `enrichment_timestamp` and `region` columns while
   v1 rows have NULLs. See [Schema Revision ID Behavior](#schema-revision-id-behavior)
   for why `schema_revision_id` does not reliably differentiate the two.

### Validation Queries

After the pipeline processes mixed v1/v2 messages, run these queries to verify. The
queries use `enrichment_timestamp IS NOT NULL` as the v1/v2 discriminator -- see
[Schema Revision ID Behavior](#schema-revision-id-behavior) for why `schema_revision_id`
does not reliably distinguish the two.

```sql
-- 1. Mixed traffic overview: count v1 vs v2 messages
SELECT
  CASE WHEN enrichment_timestamp IS NOT NULL THEN 'v2 (enriched)' ELSE 'v1 (plain)' END AS schema_version,
  COUNT(*) AS message_count,
  COUNT(DISTINCT ride_id) AS unique_rides,
  MIN(publish_time) AS first_seen,
  MAX(publish_time) AS last_seen
FROM `your-project-id.demo_dataset.taxi_events_schema`
GROUP BY schema_version;
```

```sql
-- 2. Sample v1 and v2 rows side by side
SELECT
  CASE WHEN enrichment_timestamp IS NOT NULL THEN 'v2' ELSE 'v1' END AS version,
  ride_id, ride_status, latitude,
  enrichment_timestamp, region, publish_time
FROM `your-project-id.demo_dataset.taxi_events_schema`
ORDER BY publish_time DESC
LIMIT 20;
```

```sql
-- 3. Per-minute v1/v2 mix timeline
SELECT
  TIMESTAMP_TRUNC(publish_time, MINUTE) AS minute,
  COUNTIF(enrichment_timestamp IS NOT NULL) AS v2_count,
  COUNTIF(enrichment_timestamp IS NULL) AS v1_count,
  COUNT(*) AS total
FROM `your-project-id.demo_dataset.taxi_events_schema`
GROUP BY minute
ORDER BY minute DESC
LIMIT 15;
```

```sql
-- 4. v2 region distribution
SELECT
  region,
  COUNT(*) AS count
FROM `your-project-id.demo_dataset.taxi_events_schema`
WHERE enrichment_timestamp IS NOT NULL
GROUP BY region
ORDER BY count DESC;
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
(with extra fields like `enrichment_timestamp` and `region`), **the publish succeeds**.
Avro's forward compatibility rules allow extra fields -- the v1 schema simply ignores
fields it does not define.

This was confirmed empirically: publishing 21,034 v2 messages (with `enrichment_timestamp`
and `region`) to a v1-only topic produced **zero errors**. The extra fields are stored in
Pub/Sub as part of the JSON payload but are invisible to the v1 pipeline (which only
extracts the 9 fields it knows about from the v1 schema).

The validation flow:

1. Mirror publisher (from Sub B with SMT) calls `publish()` with an enriched JSON payload
   containing 11 fields (9 original + 2 new).
2. The schema topic checks its allowed revision range -- only v1 is configured.
3. Pub/Sub validates the JSON against the v1 Avro schema (9 fields).
4. The v2 message has extra fields (`enrichment_timestamp`, `region`) not defined in v1.
5. **Validation passes.** Avro treats unknown fields as acceptable (forward compatibility).
   The message is stored with all 11 fields intact.
6. The v1 pipeline reads the message and extracts only the 9 v1 fields. The extra fields
   are present in the JSON but ignored by the DoFn's schema-driven loop.

What the schema registry **does** enforce:

- **Required fields must be present.** A message missing `passenger_count` (required in v1)
  is rejected with `INVALID_ARGUMENT`.
- **Field types must be correct.** A message with `point_idx` as a string instead of an
  integer is rejected with `INVALID_ARGUMENT`.

What the schema registry **does not** enforce:

- **Extra fields are not rejected.** A message with additional fields beyond what the schema
  defines is accepted per Avro's schema resolution rules.

This means the schema registry enforces **structural contracts** (required fields, correct
types) but allows forward-compatible evolution (extra fields). The ordering dependency in
Phase 2 exists not because v2 messages would be rejected, but because the pipeline needs
the v2 schema to **extract** the new fields.

### Ordering Dependency

Although v2 messages (with extra fields) can be published to a v1-only topic without
errors (see above), the Phase 2 evolution steps still have a **strict ordering dependency**.
The reason is not message rejection but **field extraction**: the pipeline only extracts
fields that the schema it fetched at startup defines.

```
Step 1: Commit v2 schema revision         <-- Schema now has v1 AND v2 definitions
Step 2: Update topic revision range        <-- Topic validates against both v1 and v2;
                                               messages get correct googclient_schemarevisionid
   ─── GATE: Steps 1-3 must complete before step 4 ───
Step 3: Re-run deployment script           <-- Auto-cancels old job, updates BQ table
                                               (adds 2 new columns), submits new pipeline
                                               that fetches v2 schema (11 fields)
Step 4: Start v2 mirror publisher          <-- v2 messages now have their fields extracted
```

If you start the v2 mirror publisher (Step 4) before updating the schema and restarting
the pipeline (Steps 1-3), the v2 messages **are accepted** by the topic but the pipeline
**ignores the new fields**. The `enrichment_timestamp` and `region` values are present in
the JSON payload but the DoFn's field loop only iterates over the 9 v1 field names. The
new fields are silently lost -- they pass through Pub/Sub but never reach BigQuery.

Additionally, without Step 2, the topic only allows the v1 revision. After Step 2, the
topic accepts messages matching any revision in the v1-to-v2 range. In practice, due to
Avro's forward/backward compatibility, Pub/Sub may stamp all messages with the same
revision ID regardless of which fields are present (see
[Schema Revision ID Behavior](#schema-revision-id-behavior)). The primary value of Step 2
is formally declaring v2 as an accepted schema revision -- governance, not stamping.

This ordering ensures:

1. **Schema governance**: The v2 revision is explicitly declared and authorized.
2. **Complete extraction**: The pipeline has the v2 field list and extracts all 11 fields.
3. **Governance audit trail**: Each message's `schema_revision_id` confirms that schema
   governance was active at publish time. While it may not differentiate v1 from v2 in
   practice (see [Schema Revision ID Behavior](#schema-revision-id-behavior)), it proves
   the message was validated against a known revision.

### Schema Revision ID Behavior

During live testing with mixed v1/v2 traffic, all messages received the **same
`schema_revision_id`** (the v1 revision ID) -- even after committing the v2 revision,
updating the topic's revision range, and restarting the pipeline.

**Why this happens:** Pub/Sub evaluates schema revisions from newest to oldest and stamps
the message with the first matching revision. Because v2 adds only nullable fields with
defaults, both v1 and v2 messages are valid against **both** revisions under Avro's
forward/backward compatibility rules. Pub/Sub may consistently match one revision for all
messages regardless of which fields are present. This is server-side Pub/Sub behavior, not
a pipeline code issue.

**Practical consequence:** `schema_revision_id` cannot be used to distinguish v1 from v2
messages. The reliable discriminator is `enrichment_timestamp IS NOT NULL`:

- **v1 messages:** `enrichment_timestamp` is `NULL` (field not present in payload, stored
  as NULL via `.get()` default).
- **v2 messages:** `enrichment_timestamp` is populated by the SMT with an ISO 8601 timestamp.

**What `schema_revision_id` still tells you:**

- Schema governance is active -- the field is populated on every message.
- The message was validated against a known schema revision at publish time.
- The schema name (`googclient_schemaname`) confirms which schema resource was used.

The validation queries in this document use `enrichment_timestamp IS NOT NULL` as the v1/v2
discriminator based on this observed behavior.

## Key Design Decisions

### Why Schema-Driven (Not Hardcoded or Raw JSON)

This repository provides three ingestion patterns. The schema-driven pipeline occupies a
specific niche between the other two:

| Aspect | Standard ETL | Raw JSON ELT | Schema-Driven ETL |
|---|---|---|---|
| Field extraction | Hardcoded `.get()` calls | None (raw payload) | Dynamic loop over schema fields |
| BQ schema | Hardcoded function | Single `JSON` column | Generated from Avro at startup |
| Schema change impact | Code + deploy + ALTER TABLE | None | Job restart only |
| Source of truth | Python code | N/A | Pub/Sub Schema Registry |
| Data loss risk | Possible | None | None |
| Publish-time validation | No | No | Yes |
| BQ query complexity | Low | Medium (JSON functions) | Low |

**When to use Raw JSON (ELT) instead:** For most streaming ingestion use cases, the Raw JSON
ELT pattern (`pipeline_json.py`) is simpler and more resilient. It decouples ingestion from
transformation, preserves raw data, and handles schema changes with zero pipeline impact.
Schema evolution becomes a BigQuery concern (update views or scheduled queries) rather than a
pipeline concern. See the [README](../README.md) for a full comparison.

**When to use Schema-Driven instead:** When schema governance is a hard requirement --
multi-team environments where producers and consumers need a formal contract, regulatory
compliance requiring publish-time validation, or when you want typed BigQuery columns with
automatic schema evolution and zero code changes on the pipeline side.

All three pipelines coexist in the repo as different ingestion patterns:
- `pipeline.py` + `run_dataflow.sh`: Standard ETL with manual parsing.
- `pipeline_json.py` + `run_dataflow_json.sh`: Raw JSON ELT (recommended default).
- `pipeline_schema_driven.py` + `run_dataflow_schema_driven.sh`: Schema-driven ETL with
  registry governance.

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

### Custom Logical Type for Timestamps

The public taxi data sends timestamps as ISO 8601 strings (e.g.,
`"2026-01-15T05:45:49.16882-05:00"`). JSON has no native timestamp type -- timestamps are
always serialized as strings or numbers. With JSON encoding on the Pub/Sub topic, the Avro
schema should describe the actual wire format.

Three approaches were considered:

**Option A: Avro `string` type, BQ `STRING` column.**
Simplest -- no conversion anywhere. But the schema loses semantic information (nothing says
"this string is a timestamp"), and the BQ column is STRING, losing partitioning and native
timestamp function benefits.

**Option B: Avro `timestamp-millis` logical type (`long` base), BQ `TIMESTAMP` column.**
Semantically rich, but requires the mirror publisher to convert ISO 8601 strings to epoch
milliseconds before publishing. This forces business logic into what should be a dumb relay.

**Option C: Custom Avro logical type `iso-datetime` (`string` base), BQ `TIMESTAMP` column (chosen).**
Uses Avro's support for custom logical types to annotate a `string` field as a timestamp:

```json
{"name": "timestamp", "type": {"type": "string", "logicalType": "iso-datetime"}}
```

This approach gives us the best of both worlds:

- **Mirror publisher stays a pure pass-through.** The source sends a string, the Avro schema
  expects a string, no conversion needed.
- **Schema carries semantic information.** The `logicalType` annotation tells downstream
  consumers (our pipeline code) that this string contains a timestamp, not arbitrary text.
- **Pipeline maps it to BQ `TIMESTAMP`.** The `_AVRO_LOGICAL_TO_BQ` mapping includes
  `"iso-datetime": "TIMESTAMP"`, so the type mapper automatically generates a BQ TIMESTAMP
  column. The DoFn parses the ISO 8601 string and converts it to a Beam `Timestamp` object.
- **Pub/Sub validates the base type.** The schema registry sees `string` and validates
  that the value is a string. Custom logical types are valid Avro and silently ignored by
  libraries that don't understand them -- they fall back to the base type.

**Important caveat:** Pub/Sub does not validate that the string is a valid ISO 8601 timestamp.
It only checks that it's a string. Semantic validation (is this a parseable timestamp?) happens
in the pipeline DoFn. This is acceptable because:

1. The source data (public taxi topic) always sends valid ISO 8601 timestamps.
2. The schema registry's role is structural validation (correct fields, correct types),
   not semantic validation (is this string a valid date?).
3. If invalid timestamp strings appear, the pipeline DoFn can handle the error gracefully.

This pattern is extensible. Other custom logical types (e.g., `iso-date`, `email`, `uuid`)
could be added to annotate string fields with semantic meaning while keeping the Avro base
type as `string` for JSON encoding compatibility.

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

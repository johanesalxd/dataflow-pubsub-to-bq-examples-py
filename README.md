# Streaming Pub/Sub to BigQuery: Three Ingestion Patterns

Apache Beam pipelines (Python) demonstrating three approaches to streaming data from Google Cloud Pub/Sub to BigQuery. Each pattern makes different trade-offs between pipeline complexity, schema flexibility, and data governance.

All three pipelines read from the same public taxi ride topic (`projects/pubsub-public-data/topics/taxirides-realtime`) and write to BigQuery using the Storage Write API.

## Choosing a Pattern

| Criteria | Standard (ETL) | Raw JSON (ELT) | Schema-Driven (ETL) |
|---|---|---|---|
| Pipeline complexity | Medium | Low | Medium |
| Schema change impact | Code + deploy + ALTER TABLE | None | Job restart only |
| Data loss risk on schema change | Possible | None | None |
| BigQuery query complexity | Low (typed columns) | Medium (JSON functions) | Low (typed columns) |
| Infrastructure required | Pub/Sub + Dataflow | Pub/Sub + Dataflow | Pub/Sub + Schema Registry + Dataflow |
| Replay / reprocess | Requires code changes | Re-create views | Re-run deployment script |
| Publish-time validation | No | No | Yes (schema registry) |
| Schema governance | Implicit (in code) | None | Explicit (registry revisions) |

## Pattern 1: Standard ETL (Flattened)

The pipeline parses each JSON message field-by-field and writes to typed BigQuery columns. Field extraction and the BQ schema are defined in application code.

```bash
./run_dataflow.sh
```

**How it works:**
- Pipeline reads JSON messages from Pub/Sub
- DoFn parses each field (`ride_id`, `latitude`, `timestamp`, etc.) with explicit `.get()` calls
- Timestamp strings are parsed from ISO 8601 to BigQuery `TIMESTAMP`
- Malformed messages are routed to a Dead Letter Queue (DLQ) table

**Pros:**
- Typed BigQuery columns for direct querying and partitioning
- Familiar ETL pattern -- transformation happens in the pipeline
- Simple BQ queries without JSON path functions

**Cons:**
- Adding or changing a field requires editing the transform code, updating the BQ schema function, modifying the deployment script, and running `ALTER TABLE`
- Data can be lost if schema changes before the pipeline is updated -- new fields are silently dropped, removed fields cause parsing errors
- DLQ needed for malformed messages

**When to use:** Small teams with stable schemas, or when you need the simplest possible BigQuery queries and don't expect frequent schema changes.

## Pattern 2: Raw JSON (ELT) -- Recommended

The pipeline ingests the raw JSON payload into a BigQuery `JSON` column with no transformation. All parsing and transformation happens in BigQuery using views, scheduled queries, or `CREATE TABLE AS SELECT`.

```bash
./run_dataflow_json.sh
```

**How it works:**
- Pipeline reads JSON messages from Pub/Sub
- DoFn validates JSON and stores the raw string in a `payload` column (type `JSON`)
- No field parsing, no type mapping, no schema awareness in the pipeline
- BigQuery handles all transformation downstream

**Pros:**
- Simplest pipeline code -- no parsing logic, no schema definitions
- Zero data loss -- raw payload is preserved regardless of upstream schema changes
- Schema evolution is a BigQuery concern, not a pipeline concern: add a view or scheduled query when the schema changes, and historical data is still accessible
- Easy replay: re-create views or materialized tables when requirements change
- Clean separation of concerns: the pipeline delivers data reliably, BigQuery handles transformation
- No pipeline redeployment needed when the source schema changes

**Cons:**
- BigQuery queries require JSON path functions (`JSON_VALUE`, `JSON_QUERY`) to extract fields
- Slightly higher storage cost (JSON overhead vs typed columns)
- Requires downstream views or materialized tables for typed access

**When to use:** Most streaming ingestion use cases. Particularly valuable when schemas change frequently, when you need to preserve raw data for audit or reprocessing, or when you want to enforce an ELT pattern where BigQuery is the transformation engine.

**Example: creating a typed view over raw JSON data:**

```sql
CREATE OR REPLACE VIEW `project.dataset.taxi_events_view` AS
SELECT
  subscription_name,
  message_id,
  publish_time,
  processing_time,
  JSON_VALUE(payload, '$.ride_id') AS ride_id,
  CAST(JSON_VALUE(payload, '$.point_idx') AS INT64) AS point_idx,
  CAST(JSON_VALUE(payload, '$.latitude') AS FLOAT64) AS latitude,
  CAST(JSON_VALUE(payload, '$.longitude') AS FLOAT64) AS longitude,
  TIMESTAMP(JSON_VALUE(payload, '$.timestamp')) AS timestamp,
  CAST(JSON_VALUE(payload, '$.meter_reading') AS FLOAT64) AS meter_reading,
  CAST(JSON_VALUE(payload, '$.meter_increment') AS FLOAT64) AS meter_increment,
  JSON_VALUE(payload, '$.ride_status') AS ride_status,
  CAST(JSON_VALUE(payload, '$.passenger_count') AS INT64) AS passenger_count
FROM `project.dataset.taxi_events_json`;
```

When the source adds new fields, update the view -- no pipeline changes needed, and all historical raw data is immediately accessible with the new schema.

## Pattern 3: Schema-Driven ETL (Pub/Sub Schema Registry)

The pipeline fetches an Avro schema from the Pub/Sub Schema Registry at startup and dynamically generates both the BigQuery table schema and the field extraction logic. Schema evolution requires zero pipeline code changes -- commit a new schema revision, update the BQ table, and restart the job.

```bash
./run_dataflow_schema_driven.sh
# In a separate terminal:
./scripts/run_mirror_publisher.sh
```

**How it works:**
- An Avro schema is registered in the Pub/Sub Schema Registry
- A mirror publisher bridges the public topic to a schema-validated topic (pure pass-through)
- Pub/Sub validates every message at publish time -- messages with missing required fields or wrong types are rejected before entering the system (extra fields are accepted per Avro forward compatibility)
- The pipeline fetches the Avro schema at startup, generates BQ columns from it, and extracts fields dynamically
- No DLQ needed because schema validation happens at publish time

**Pros:**
- Schema registry is the single source of truth -- no hardcoded field lists
- Schema evolution with zero code changes: commit a new revision, restart the job
- Publish-time validation prevents bad data from entering the system
- Schema governance: explicit versioning, revision tracking, cross-team contracts
- Each BigQuery row records which schema revision validated it (`schema_revision_id`)

**Cons:**
- More infrastructure: schema registry, schema-enabled topic, mirror publisher
- Still an ETL pattern -- transformation happens in the pipeline, not in BigQuery
- Mirror publisher required to bridge the public topic to the schema-enabled topic
- Custom `iso-datetime` logical type needed for ISO 8601 timestamp mapping

**When to use:** When schema governance is a requirement (multi-team environments, regulatory compliance), when you need publish-time validation to guarantee data quality, or when you want automatic schema evolution without code changes but prefer typed BigQuery columns over raw JSON.

**Verifying Mixed v1/v2 Traffic:**

After running the schema evolution demo (v1 + v2 mirror publishers), query BigQuery to confirm both message types are flowing. Use `enrichment_timestamp IS NOT NULL` to distinguish v2 (enriched) from v1 (plain) messages:

```sql
-- Mixed traffic overview
SELECT
  CASE WHEN enrichment_timestamp IS NOT NULL THEN 'v2 (enriched)' ELSE 'v1 (plain)' END AS schema_version,
  COUNT(*) AS message_count,
  COUNT(DISTINCT ride_id) AS unique_rides
FROM `your-project-id.demo_dataset.taxi_events_schema`
GROUP BY schema_version;
```

```sql
-- Per-minute v1/v2 timeline
SELECT
  TIMESTAMP_TRUNC(publish_time, MINUTE) AS minute,
  COUNTIF(enrichment_timestamp IS NOT NULL) AS v2_count,
  COUNTIF(enrichment_timestamp IS NULL) AS v1_count
FROM `your-project-id.demo_dataset.taxi_events_schema`
GROUP BY minute
ORDER BY minute DESC
LIMIT 15;
```

For the full set of validation queries and details on schema revision ID behavior, see [docs/schema_evolution_plan.md](docs/schema_evolution_plan.md).

## Input Data Format

All three pipelines read taxi ride events from the public Pub/Sub topic `projects/pubsub-public-data/topics/taxirides-realtime`:

```json
{
  "ride_id": "cd3b816b-bc33-4893-a232-ea7cbbb9d0e8",
  "point_idx": 104,
  "latitude": 40.76011,
  "longitude": -73.97316,
  "timestamp": "2026-02-10T08:14:17.61844-05:00",
  "meter_reading": 4.8108845,
  "meter_increment": 0.046258505,
  "ride_status": "enroute",
  "passenger_count": 6
}
```

The `timestamp` field is an ISO 8601 string with timezone offset.

## Project Structure

```
dataflow-pubsub-to-bq-examples-py/
├── README.md
├── AGENTS.md                              # Guidelines for AI agents
├── pyproject.toml                         # Project configuration (uv)
├── run_dataflow.sh                        # Deployment: Standard ETL
├── run_dataflow_json.sh                   # Deployment: Raw JSON ELT
├── run_dataflow_schema_driven.sh          # Deployment: Schema-driven ETL
├── schemas/
│   ├── taxi_ride_v1.avsc                  # Avro v1 schema definition
│   └── taxi_ride_v2.avsc                  # Avro v2 schema (adds enrichment fields)
├── scripts/
│   ├── cleanup_schema_driven.sh          # Teardown all schema-driven resources
│   ├── enrich_taxi_ride.yaml             # SMT definition for v2 enrichment
│   ├── generate_bq_schema.py             # BQ schema generator from registry
│   ├── publish_to_schema_topic.py        # Mirror publisher (pass-through relay)
│   ├── run_mirror_publisher.sh           # v1 mirror publisher launcher
│   └── run_schema_evolution.sh           # Phase 2: schema evolution + v2 publisher
├── docs/
│   └── schema_evolution_plan.md           # Schema evolution design doc
├── tests/
│   ├── test_json_to_tablerow.py           # Standard pipeline tests
│   ├── test_raw_json.py                   # JSON pipeline tests
│   └── test_schema_driven_to_tablerow.py  # Schema-driven pipeline tests
└── dataflow_pubsub_to_bq/
    ├── __init__.py
    ├── pipeline.py                        # Entry point: Standard ETL
    ├── pipeline_json.py                   # Entry point: Raw JSON ELT
    ├── pipeline_schema_driven.py          # Entry point: Schema-driven ETL
    ├── pipeline_options.py                # Options: Standard + JSON
    ├── pipeline_schema_driven_options.py  # Options: Schema-driven
    └── transforms/
        ├── __init__.py
        ├── json_to_tablerow.py            # Standard: JSON field extraction
        ├── raw_json.py                    # JSON: raw payload storage
        └── schema_driven_to_tablerow.py   # Schema-driven: Avro-based extraction
```

## Prerequisites

- Google Cloud Project with billing enabled
- APIs enabled: Dataflow, Pub/Sub, BigQuery, Cloud Storage
- gcloud CLI configured
- Python 3.12+
- uv package manager

## Setup

### Install uv (if not installed)

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Or on macOS/Linux with Homebrew:

```bash
brew install uv
```

### Install Project Dependencies

```bash
cd dataflow-pubsub-to-bq-examples-py
uv sync
```

This will create a virtual environment and install all dependencies defined in `pyproject.toml`.

## Testing

Run the pytest suite:

```bash
uv run pytest
```

## Deployment

### Pattern 1: Standard ETL

```bash
./run_dataflow.sh
```

This script will:
1. Check/create GCS bucket for temp/staging
2. Check/create BigQuery dataset and tables (main + DLQ)
3. Check/create Pub/Sub subscription
4. Install Python dependencies and build wheel
5. Submit pipeline to Dataflow with Runner V2

### Pattern 2: Raw JSON ELT

```bash
./run_dataflow_json.sh
```

Same infrastructure setup as Pattern 1. The output table is `taxi_events_json` with a `payload` column of type `JSON`.

### Pattern 3: Schema-Driven ETL

```bash
./run_dataflow_schema_driven.sh
```

This script will:
1. Create Pub/Sub schema from `schemas/taxi_ride_v1.avsc` (first run only)
2. Create schema-enabled topic and subscriptions
3. Fetch schema from registry and generate BQ table schema dynamically
4. Submit pipeline to Dataflow

Then start the mirror publisher in a separate terminal:

```bash
./scripts/run_mirror_publisher.sh
```

The mirror publisher bridges the public taxi topic to the schema-validated topic. It runs as a separate process and should remain running alongside the Dataflow job.

## Performance Optimizations

### BigQuery Storage Write API

All three pipelines use the **BigQuery Storage Write API** for optimal performance:

| Feature | Benefit |
|---------|---------|
| Auto-batching | Batches rows internally before committing |
| Exactly-once semantics | Built-in deduplication |
| Lower cost | First 2 TB/month free, then $0.025/GB (vs $0.05/GB for legacy streaming) |
| Higher throughput | Better for high-volume streaming (2,000+ RPS) |
| Micro-batching | `triggering_frequency=1` flushes every 1 second |

**Implementation:**
```python
bigquery.WriteToBigQuery(
    method=bigquery.WriteToBigQuery.Method.STORAGE_WRITE_API,
    triggering_frequency=1,  # Flush every 1 second
)
```

This provides automatic micro-batching without manual `GroupIntoBatches` transforms.

### Alternative: Legacy Streaming API with Count-Based Batching

If you need to use the **legacy STREAMING_INSERTS API** with count-based batching (e.g., "100 records OR 1 second, whichever comes first"), you can use `GroupIntoBatches`:

```python
import random
from apache_beam.transforms.util import GroupIntoBatches

(messages
 | 'ParseMessages' >> beam.ParDo(
     ParsePubSubMessage(custom_options.subscription_name)
 )
 | 'AddRandomKeys' >> beam.Map(lambda x: (random.randint(0, 4), x))
 | 'BatchMessages' >> GroupIntoBatches.WithShardedKey(
     batch_size=100,
     max_buffering_duration_secs=1,
 )
 | 'ExtractBatches' >> beam.FlatMapTuple(lambda k, v: v)
 | 'WriteToBigQuery' >> bigquery.WriteToBigQuery(
     table=custom_options.output_table,
     schema={'fields': get_bigquery_schema()},
     write_disposition=bigquery.BigQueryDisposition.WRITE_APPEND,
     create_disposition=bigquery.BigQueryDisposition.CREATE_IF_NEEDED,
     method=bigquery.WriteToBigQuery.Method.STREAMING_INSERTS,
 ))
```

**Trade-offs:**
- **Pros:** Exact control over batch size (100 records OR 1 second trigger)
- **Cons:** Higher cost ($0.05/GB vs $0.025/GB), requires sharding for parallelism, more complex code
- **When to use:** When you need exact count-based triggering or are using legacy systems

For production workloads, the Storage Write API is strongly recommended over legacy streaming inserts.

## Performance Results

### Production Test Results

Tested with the Standard ETL pipeline on the configuration below:

| Metric | Result |
|--------|--------|
| **Target Throughput** | 2,000 RPS |
| **Sustained Throughput** | **3,400+ RPS** |
| **Performance vs Target** | **170% (exceeds target)** |
| **Sustained Range** | 3,300 - 3,500 RPS (stable) |
| **Peak Throughput (Backlog Catchup)** | **58,630+ RPS** |
| **Peak Scaling Factor** | **29x target, 17x sustained** |

**Metrics Dashboard:**

![Dataflow Metrics - Peak Throughput](images/metrics_py.jpg)

**Key Findings:**
- The Python SDK with Storage Write API and Runner V2 easily handles high-volume streaming workloads (2k+ RPS)
- During backlog catchup scenarios, the pipeline can scale to **58k+ RPS** to clear accumulated messages
- Demonstrates excellent burst capacity for handling traffic spikes or recovering from downtime

## Scaling Analysis

### Dataflow Runner V2 Configuration

The production deployment uses these settings:

| Setting | Value | Notes |
|---------|-------|-------|
| Runner | DataflowRunner with Runner V2 | Required for Python SDK |
| Machine Type | n2-standard-4 | 4 vCPUs, 16 GB RAM |
| Workers | 5 (fixed) | No auto-scaling |
| Threads per Worker | 48 (default) | 12 threads per vCPU x 4 vCPUs |
| Total Capacity | 240 threads | 5 workers x 48 threads |
| Actual Load | ~3,400 RPS | From Pub/Sub subscription |
| Per-Thread Load | ~14 messages/sec | 3,400 / 240 threads |

### Thread Configuration

The Python SDK on Dataflow uses **12 threads per vCPU** by default. For n2-standard-4 machines (4 vCPUs), this means:
- 12 threads/vCPU x 4 vCPUs = **48 threads per worker**
- 5 workers x 48 threads = **240 total threads**

This is sufficient for processing 3,400+ RPS with low system lag.

### Monitoring

Monitor the pipeline performance at:

- Dataflow console: https://console.cloud.google.com/dataflow
- BigQuery: https://console.cloud.google.com/bigquery

Key metrics to watch:
- System lag (data freshness)
- Throughput (elements/sec)
- CPU utilization per worker
- Memory usage per worker

## Configuration

Update the project-specific variables (project ID, region, bucket, dataset,
subscription) at the top of each `run_dataflow*.sh` script before deployment.

## License

This project is provided as-is for educational and testing purposes.

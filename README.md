# Pub/Sub to BigQuery Pipeline (Python & Java)

Apache Beam pipelines (Python & Java) that read taxi ride data from Google Cloud Pub/Sub and write to BigQuery.

## Architecture

```
Pub/Sub (taxirides-realtime) -> Dataflow Pipeline -> BigQuery (taxi_events)
                                                  -> BigQuery DLQ (taxi_events_dlq) [Error Handling]
```

## Features

- Reads from public Pub/Sub topic (taxirides-realtime)
- Parses taxi ride JSON messages
- Captures Pub/Sub metadata (subscription_name, message_id, publish_time, attributes)
- Writes to partitioned and clustered BigQuery table
- **Dual Pipeline Support:**
  - Standard: Flattens JSON fields into specific columns.
  - Raw JSON: Ingests the full payload into a BigQuery `JSON` column.
- Uses BigQuery Storage Write API for optimal performance
- Micro-batching with 1-second flush intervals
- Production deployment via Dataflow (DataflowRunner with Runner V2)

## Project Structure

```
dataflow-pubsub-to-bq-examples-py/
├── README.md                   # This file
├── AGENTS.md                   # Guidelines for AI agents
├── pyproject.toml              # Project configuration and dependencies (uv)
├── run_dataflow.sh             # Production Dataflow deployment script (Flattened Schema)
├── run_dataflow_json.sh        # Production Dataflow deployment script (JSON Column)
├── run_dataflow_json_java.sh   # Java Production Dataflow deployment script
├── java/                       # Java implementation
│   ├── pom.xml
│   └── src/
│       ├── main/java/com/johanesalxd/
│       │   ├── PubSubToBigQueryJson.java
│       │   ├── transforms/
│       │   └── schemas/
│       └── test/java/com/johanesalxd/transforms/
│           └── PubsubMessageToRawJsonTest.java
├── tests/                      # Unit tests
│   ├── test_json_to_tablerow.py
│   └── test_raw_json.py
└── dataflow_pubsub_to_bq/
    ├── __init__.py
    ├── pipeline.py             # Main pipeline code (Flattened)
    ├── pipeline_json.py        # Alternative pipeline code (Raw JSON)
    ├── pipeline_options.py     # Custom pipeline options
    └── transforms/
        ├── __init__.py
        ├── json_to_tablerow.py # JSON parsing transform (Flattened)
        └── raw_json.py         # Raw JSON parsing transform
```

## Input Data Format

The pipeline reads taxi ride events from the public Pub/Sub topic with this structure:

```json
{
  "ride_id": "7da878a4-37ce-4aa6-a899-07d4054afe96",
  "point_idx": 267,
  "latitude": 40.76561,
  "longitude": -73.96572,
  "timestamp": "2023-07-10T19:00:33.56833-04:00",
  "meter_reading": 11.62449,
  "meter_increment": 0.043537416,
  "ride_status": "enroute",
  "passenger_count": 1
}
```

## BigQuery Output Schema

```
subscription_name: STRING
message_id: STRING
publish_time: TIMESTAMP (partitioned, clustered)
processing_time: TIMESTAMP
ride_id: STRING
point_idx: INT64
latitude: FLOAT
longitude: FLOAT
timestamp: TIMESTAMP
meter_reading: FLOAT
meter_increment: FLOAT
ride_status: STRING
passenger_count: INT64
attributes: STRING
```

## Prerequisites

- Google Cloud Project with billing enabled
- APIs enabled: Dataflow, Pub/Sub, BigQuery, Cloud Storage
- gcloud CLI configured
- Python 3.12+ (for Python pipeline)
- Java 17 (for Java pipeline)
- uv package manager
- Maven 3.x

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

Unit tests are implemented for both languages to verify transformation logic and Dead Letter Queue (DLQ) routing.

### Python
Run the pytest suite:
```bash
uv run pytest
```

### Java
Run JUnit tests:
```bash
mvn -f java/pom.xml test
```

## Schema Management

### Python Implementation
The Python pipeline relies on external infrastructure management for table creation.
- **Tables must be pre-created:** The pipeline uses `CREATE_NEVER` disposition.
- **Reason:** The Python SDK's BigQueryIO connector has validation limitations with the `JSON` column type in schema definitions.
- **Automation:** The provided scripts (`run_dataflow.sh` and `run_dataflow_json.sh`) handle the creation of the main table and the partitioned Dead Letter Queue (DLQ) table.

### Java Implementation
The Java pipeline supports both methods:
- **Operational:** Can rely on pre-created tables (like the Python flow).
- **Self-Healing:** Also supports `CREATE_IF_NEEDED`. The Java SDK natively supports and validates the `JSON` column type, allowing it to create the correct schema at runtime if the table is missing.

## Usage

### Java Implementation (Comparison)

A parallel Java implementation is provided to enable direct performance comparisons with the Python version. It mirrors the Python "Raw JSON" pipeline architecture exactly:

*   **Beam SDK:** Java SDK 2.70.0 (Matching Python version)
*   **Write Method:** Storage Write API with 1-second triggering frequency.
*   **Schema Strategy:**
    *   **Operational:** The `run_dataflow_json_java.sh` script pre-creates the table using `bq mk` to ensure infrastructure state.
    *   **Application:** The Java code (`RawJsonBigQuerySchema.java`) also defines the schema and uses `CREATE_IF_NEEDED`. This redundancy ensures the pipeline is self-healing and allows Beam to validate data structures before writing.

**Note on Schema:** Both the Python and Java pipelines support the `JSON` column type. The Python implementation uses `json.dumps` to serialize the dictionary before writing, while the Java implementation passes the raw JSON string directly to the `payload` column defined as `JSON` type in BigQuery.

**Running the Java Pipeline:**

Prerequisites: Java 17, Maven 3.x

```bash
./run_dataflow_json_java.sh
```

This script handles the build (`mvn package`) and submission steps automatically.

### Production Deployment (DataflowRunner)

**Option 1: Standard Flattened Schema**

```bash
./run_dataflow.sh
```

This script will:
1. Check/create GCS bucket for temp/staging
2. Check/create BigQuery dataset
3. Check/create BigQuery table with schema
4. Check/create Pub/Sub subscription
5. Install Python dependencies
6. Submit pipeline to Dataflow with Runner V2

**Option 2: Raw JSON Column Schema**

```bash
./run_dataflow_json.sh
```

This pipeline ingests the entire message payload into a specific BigQuery `JSON` column named `payload`, along with standard metadata fields. This is useful for:
- Handling semi-structured data
- Schema evolution without table updates
- Preserving original message fidelity

The output table is `taxi_events_json`.

## Performance Optimizations

### BigQuery Storage Write API

This pipeline uses the **BigQuery Storage Write API** for optimal performance:

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

# In pipeline.py
(messages
 | 'ParseMessages' >> beam.ParDo(
     ParsePubSubMessage(custom_options.subscription_name)
 )
 | 'AddRandomKeys' >> beam.Map(lambda x: (random.randint(0, 4), x))  # 5 shards for parallelism
 | 'BatchMessages' >> GroupIntoBatches.WithShardedKey(
     batch_size=100,  # Flush after 100 records
     max_buffering_duration_secs=1,  # OR after 1 second
 )
 | 'ExtractBatches' >> beam.FlatMapTuple(lambda k, v: v)
 | 'WriteToBigQuery' >> bigquery.WriteToBigQuery(
     table=custom_options.output_table,
     schema={'fields': get_bigquery_schema()},
     write_disposition=bigquery.BigQueryDisposition.WRITE_APPEND,
     create_disposition=bigquery.BigQueryDisposition.CREATE_IF_NEEDED,
     method=bigquery.WriteToBigQuery.Method.STREAMING_INSERTS,  # Legacy API
 ))
```

**Trade-offs:**
- **Pros:** Exact control over batch size (100 records OR 1 second trigger)
- **Cons:** Higher cost ($0.05/GB vs $0.025/GB), requires sharding for parallelism, more complex code
- **When to use:** When you need exact count-based triggering or are using legacy systems

**Note:** For production workloads, the Storage Write API is strongly recommended over legacy streaming inserts.

## Performance Results

### Production Test Results

Tested with the configuration below, the pipeline achieved excellent performance:

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
| Threads per Worker | 48 (default) | 12 threads per vCPU × 4 vCPUs |
| Total Capacity | 240 threads | 5 workers × 48 threads |
| Actual Load | ~3,400 RPS | From Pub/Sub subscription |
| Per-Thread Load | ~14 messages/sec | 3,400 / 240 threads |

### Thread Configuration

The Python SDK on Dataflow uses **12 threads per vCPU** by default. For n2-standard-4 machines (4 vCPUs), this means:
- 12 threads/vCPU × 4 vCPUs = **48 threads per worker**
- 5 workers × 48 threads = **240 total threads**

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

### run_dataflow.sh
Edit these variables:
- `PROJECT_ID`: Your GCP project ID
- `REGION`: GCP region for Dataflow job
- `TEMP_BUCKET`: GCS bucket for temp/staging
- `BIGQUERY_DATASET`: BigQuery dataset name
- `SUBSCRIPTION_NAME`: Pub/Sub subscription name

## Troubleshooting

### High System Lag

If system lag is increasing:
1. Increase number of workers (`--num_workers`)
2. Use larger machine type (`--machine_type=n2-standard-8`)
3. Check BigQuery write quota limits

### OOM Errors

If workers run out of memory:
1. Reduce `--number_of_worker_harness_threads` (max 12)
2. Use machine type with more RAM
3. Check for memory leaks in transforms

### Slow BigQuery Writes

If BigQuery is the bottleneck:
1. Enable Streaming Engine (`--enable_streaming_engine`)
2. Check BigQuery streaming insert quota
3. Consider batch writes with windowing

## License

This project is provided as-is for educational and testing purposes.

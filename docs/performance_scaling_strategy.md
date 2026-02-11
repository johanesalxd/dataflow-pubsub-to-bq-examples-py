# BQ Throughput Ceiling Test

Reproducing and diagnosing the "Noisy Neighbor" throughput degradation when multiple Dataflow jobs write to the same BigQuery table.

## 1. Problem

A single Dataflow job writes to BigQuery at 100 MB/s. Adding a second identical job should give 200 MB/s total, but only 120 MB/s is observed. Both jobs show low CPU and low Dataflow lag -- no errors are visible. We need to identify the bottleneck.

## 2. BigQuery Quotas

Source: [BigQuery Quotas -- Storage Write API](https://cloud.google.com/bigquery/quotas#write-api-limits)

| Resource | Multi-Region (`us`/`eu`) | Regional (e.g., `asia-southeast1`) |
| :--- | :--- | :--- |
| **Throughput** | 3 GB/s per project | 300 MB/s per project |
| **Concurrent Connections** | 10,000 per project | 1,000 per project |
| **CreateWriteStream** | 10,000 streams/hour | 10,000 streams/hour |

**Key details:**
* Throughput quota is metered on the **destination project** (where the BQ dataset lives).
* Concurrent connections are metered on the **client project** (the Dataflow project).
* When the BQ sink is saturated, Dataflow workers **self-throttle** and stop pulling from Pub/Sub. This causes **low CPU** and **low Dataflow lag**, while the **Pub/Sub backlog grows**. The backpressure is invisible unless you check the source-side metrics.
* Each Beam worker may open 10-50 write streams. Two jobs with 20 workers each could approach the 1,000 regional connection limit.

**Pub/Sub limits relevant to this test:**
* Max message size: 10 MB.
* StreamingPull: 10 MB/s per stream. At 100 MB/s, we need at least 10 concurrent pull streams (Beam handles this automatically with multiple workers).

## 3. Test Architecture

Each job gets its own topic + subscription. Both jobs write to the **same** BigQuery table.

```
Publisher A (10k msgs/sec, ~10 KB each, 100 MB/s)
    -> Topic A (perf_test_topic_a)
        -> Sub A (perf_test_sub_a)
            -> Dataflow Job A (pipeline_json.py)
                -> BQ: demo_dataset_asia.taxi_events_perf

Publisher B (same config)
    -> Topic B (perf_test_topic_b)
        -> Sub B (perf_test_sub_b)
            -> Dataflow Job B (same pipeline, same BQ table)
```

### Why This Design

* **`pipeline_json.py`**: Stores the entire payload in a BQ `JSON` column. Minimal CPU overhead isolates the BQ write bottleneck -- we're not testing parsing performance.
* **Dedicated topic per job**: Mirrors the production Kafka scenario (each job reads from its own topic). The synthetic publisher controls the exact rate per topic.
* **Same BQ table**: Reproduces the production scenario where jobs compete for the same table.

### Message Design

| Parameter | Value |
| :--- | :--- |
| Message size | ~10,000 bytes (9 taxi ride fields + `_padding`) |
| Publish rate | 10,000 msgs/sec per publisher |
| Pub/Sub throughput | 100 MB/s per topic |
| BQ row size | ~10,128 bytes (payload + 128 bytes metadata) |
| BQ write throughput | ~101 MB/s per job |
| Two jobs combined | ~202 MB/s (67% of 300 MB/s regional quota) |

## 4. How to Run

### 4.1 Prerequisites

**Set your project ID** in both scripts:

```bash
# Edit the PROJECT_ID variable at the top of each file:
#   scripts/run_perf_test.sh
#   scripts/cleanup_perf_test.sh
```

Required GCP APIs: Dataflow, Pub/Sub, BigQuery, Cloud Storage.

### 4.2 Validate Sizes (Dry Run)

Run this first. No GCP resources are created, no cost incurred.

```bash
./scripts/run_perf_test.sh dry-run
```

Expected output:

```
=== Dry Run Report ===
Sample messages generated: 10,000
Target message size:       10,000 bytes
Actual message size:       avg=10000, min=10000, max=10000 bytes

At --target_mbps=100.0:
  Publish rate:            10,000 msgs/sec
  Pub/Sub throughput:      100.0 MB/s
  Pub/Sub streams needed:  10 (at 10 MB/s per StreamingPull)

  BQ row size (estimated): 10128 bytes (payload + 128 bytes metadata)
  BQ write throughput:     101.3 MB/s

  BQ Storage Write API quota usage (per job):
    Regional (300 MB/s):   33.8%
    Multi-region (3 GB/s): 3.4%

  Two jobs at this rate:
    Combined BQ write:     202.6 MB/s
    Regional quota usage:  67.5%

Sample message validation: OK (valid JSON, all taxi fields present)
```

Verify the numbers make sense before proceeding.

### 4.3 Setup Resources

Creates GCS bucket, BQ table, Pub/Sub topics, subscriptions, and builds the wheel.

```bash
./scripts/run_perf_test.sh setup
```

### 4.4 Phase 1 -- Single Job Baseline

Open **two terminals**. Start the publisher first, then the Dataflow job.

```bash
# Terminal 1: start publishing to Topic A
./scripts/run_perf_test.sh publisher-a

# Terminal 2: launch Dataflow Job A
./scripts/run_perf_test.sh job-a
```

Wait **15 minutes** for steady state. Record:
* Worker CPU % (Dataflow job page)
* BQ write throughput (Cloud Monitoring)
* Pub/Sub backlog for `perf_test_sub_a`

**Expected:** ~100 MB/s BQ write, CPU < 30%, backlog stable.

### 4.5 Phase 2 -- Noisy Neighbor

Keep Phase 1 running. Open **two more terminals**.

```bash
# Terminal 3: start publishing to Topic B
./scripts/run_perf_test.sh publisher-b

# Terminal 4: launch Dataflow Job B
./scripts/run_perf_test.sh job-b
```

Wait **10 minutes**. Observe:
* Does Job A's throughput drop?
* What is the combined throughput? (expecting < 200 MB/s if contention exists)
* Check BQ quota and connection usage (see Section 5)

Print all monitoring URLs:

```bash
./scripts/run_perf_test.sh monitor
```

### 4.6 Scaling Up

To test with more workers, clean up the current round and re-run with a new `NUM_WORKERS` value.

```bash
# 1. Stop publishers (Ctrl+C in terminals 1 and 3)
# 2. Clean up all resources
./scripts/cleanup_perf_test.sh --force

# 3. Edit NUM_WORKERS in scripts/run_perf_test.sh
#    Change: NUM_WORKERS=3  ->  NUM_WORKERS=10

# 4. Re-run from setup
./scripts/run_perf_test.sh setup
# Then repeat 4.4 and 4.5
```

Recommended test rounds:

| Round | Workers per Job | Total Workers | Purpose |
| :--- | :--- | :--- | :--- |
| 1 | 3 | 6 | Baseline -- find single-job ceiling with minimal resources |
| 2 | 10 | 20 | Does throughput scale linearly with workers? |
| 3 | 20 | 40 | Push toward connection limit (40 workers x ~25 streams = ~1,000) |

### 4.7 Cleanup

Cancels Dataflow jobs, deletes topics, subscriptions, and BQ tables.

```bash
# Interactive (asks for confirmation)
./scripts/cleanup_perf_test.sh

# Skip confirmation
./scripts/cleanup_perf_test.sh --force
```

## 5. Monitoring

### Key Metrics

| Metric | Where to Find | What It Means |
| :--- | :--- | :--- |
| BQ write throughput | Cloud Monitoring: `bigquerystorage.googleapis.com/write/append_bytes` | Actual bytes/sec written |
| Concurrent connections | Cloud Monitoring: `bigquerystorage.googleapis.com/write/max_active_streams` | Active write streams |
| Pub/Sub backlog | Cloud Monitoring: `pubsub.googleapis.com/subscription/num_undelivered_messages` | Growing = backpressure |
| Worker CPU | Dataflow job monitoring tab | < 30% = I/O bound, not compute bound |
| BQ write wall time | Dataflow > Execution Details > WriteToBigQuery stage | Increasing = BQ contention |

### Quota Dashboard

```
https://console.cloud.google.com/iam-admin/quotas?project=PROJECT_ID&metric=bigquerystorage.googleapis.com
```

Filter by:
* `AppendBytesThroughputPerProjectRegion` -- throughput
* `ConcurrentWriteConnectionsPerProjectRegion` -- connections

### Interpreting Results

**Healthy (no contention):**
* Pub/Sub backlog stable, CPU 10-30%, BQ throughput ~100 MB/s per job, scales linearly.

**Backpressure (BQ-side bottleneck):**
* Pub/Sub backlog growing, CPU low (< 20%), Dataflow system lag low (misleading), BQ write wall time increasing.

**Key insight:** If CPU is low but lag grows, the bottleneck is I/O, not compute. Bigger workers will not help.

## 6. Decision Matrix

| Combined Throughput | BQ Quota Usage | Connections | Diagnosis | Action |
| :--- | :--- | :--- | :--- | :--- |
| ~200 MB/s | < 100% | < 1,000 | No contention | System works as expected |
| ~120 MB/s | ~100% | < 1,000 | **Throughput quota** | Request quota increase |
| ~120 MB/s | < 100% | ~1,000 | **Connection limit** | Request connection quota increase or enable multiplexing |
| ~120 MB/s | < 100% | < 1,000 | **Per-table contention** | Write to separate tables or change partitioning |

## 7. Synthetic Publisher Reference

The publisher can also be invoked directly for advanced use cases.

```bash
uv run python scripts/synthetic_publisher.py \
    --project=my-project \
    --topic=projects/my-project/topics/my-topic \
    --target_mbps=100 \
    --message_size_bytes=10000 \
    --duration_minutes=30
```

| Flag | Default | Description |
| :--- | :--- | :--- |
| `--project` | (required) | GCP project ID |
| `--topic` | (required) | Full Pub/Sub topic path |
| `--target_mbps` | `100` | Target publish rate in MB/s |
| `--message_size_bytes` | `10000` | Target size per message in bytes (min: 250) |
| `--duration_minutes` | `30` | How long to run (0 = infinite) |
| `--dry-run` | `false` | Validate sizes and math without publishing |

**Broker-agnostic design:** The `generate_message()` and `calculate_rates()` functions have zero Pub/Sub dependency. They return plain strings and dicts that work with any message broker.

## 8. Kafka Migration

To reproduce this test with Kafka instead of Pub/Sub:

| Component | Pub/Sub (current) | Kafka (swap) |
| :--- | :--- | :--- |
| Publisher | `pubsub_v1.PublisherClient` | `confluent_kafka.Producer` |
| Pipeline source | `ReadFromPubSub(subscription=...)` | `ReadFromKafka(consumer_config=..., topics=...)` |
| Message format | JSON string | JSON string or Avro binary |
| BQ write | Unchanged | Unchanged |
| Message generation | `generate_message()` -- reuse as-is | Same function |
| Rate calculation | `calculate_rates()` -- reuse as-is | Same function |

To add Avro encoding (reproducing the 24 MB/s Avro -> 100 MB/s JSON expansion), serialize the `generate_message()` output with `fastavro` in the publisher and add deserialization in the pipeline.

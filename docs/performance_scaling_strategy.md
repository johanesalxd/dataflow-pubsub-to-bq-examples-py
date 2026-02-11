# Dataflow & BigQuery Performance: Scaling, Bottlenecks, and Optimization

This document summarizes the technical strategy for diagnosing and optimizing Dataflow-to-BigQuery pipelines, specifically addressing the "Noisy Neighbor" theory and the limits of Vertical Scaling.

## 1. The "Noisy Neighbor" Hypothesis
**Observation:** Job A reaches 100 MBps. Adding Job B causes both to total only 120 MBps (Job A drops to ~60 MBps). No "errors" are visible, and Dataflow lag appears low.

**The Reality:**
The bottleneck is likely a **shared resource at the BigQuery sink layer**. Candidates include:
* **Throughput Quota:** All Dataflow jobs in the same project and region share a single Storage Write API throughput quota.
* **Concurrent Connections:** Each Beam worker opens multiple write streams. Two jobs double the connection count, which may hit the per-project connection limit.
* **Per-Table Contention:** Two jobs writing to the same table compete for table-level write locks and partition slots.
* **Backpressure:** When the BigQuery sink is saturated, Dataflow workers self-throttle and stop pulling data from the source. This is why you see **Low CPU** and **Low Dataflow Lag**, while the **Source Lag (Pub/Sub/Kafka)** actually increases.

## 2. Horizontal vs. Vertical Scaling

| Strategy | When it Works | Why it Fails for I/O |
| :--- | :--- | :--- |
| **Vertical Scaling** (Bigger Workers) | CPU-intensive logic (complex crypto, heavy parsing). | **I/O Latency:** A bigger CPU doesn't make the network ACK from BigQuery come back faster. |
| **Horizontal Scaling** (More Workers) | I/O-bound or Latency-bound tasks. | **Quota Caps:** It works until you hit the shared Project-level quota at the Sink. |

**The Proof:** If you increase worker size and CPU utilization drops even lower while lag stays the same, you have a **Wait-State Bottleneck** (I/O).

## 3. BigQuery Storage Write API Limits

Official quotas (source: [BigQuery Quotas](https://cloud.google.com/bigquery/quotas#write-api-limits)):

| Resource | Multi-Region (`us`/`eu`) | Regional (e.g., `asia-southeast1`) |
| :--- | :--- | :--- |
| **Throughput** | 3 GB/s per project | 300 MB/s per project |
| **Concurrent Connections** | 10,000 per project | 1,000 per project |
| **CreateWriteStream** | 10,000 streams/hour | 10,000 streams/hour |

**Key details:**
* Throughput quota is metered on the **destination project** (where the BQ dataset resides), not the client project.
* Concurrent connections are metered on the **client project** (the project running the Dataflow job).
* You may not see `429: Quota Exceeded` if the Beam client library is successfully self-throttling via exponential backoff on `RESOURCE_EXHAUSTED` errors.
* The connection quota can be a hidden bottleneck: each Beam worker may open 10-50 write streams depending on parallelism. Two jobs with 20 workers each could open 400-2,000 streams, approaching or exceeding the 1,000 regional limit.

## 4. View vs. Materialized View (MV) Optimization

### Standard Views
* **Pruning:** BigQuery pushes filters (Predicates) down through views to the base table.
* **Requirement:** Pruning works as long as the partitioning/clustering columns are **not transformed** inside the view (e.g., don't `CAST` or `CONCAT` the partition key).

### Materialized Views
* **Heavy Lifting:** Use for complex joins/aggregates to pre-compute results.
* **Clustering:** You can cluster an MV differently than the base table (a "second dimension" for pruning).
* **Staleness (`max_staleness`):**
    * **Default:** Always fresh (merges MV with base table delta at query time).
    * **Staleness Window:** If you set a 30m window, BigQuery skips the base table delta if the MV refresh is recent, saving significant cost/latency.

## 5. Experimental Reproduction Plan

### Goal
Reproduce the "Noisy Neighbor" throughput degradation in a controlled environment and identify whether the bottleneck is throughput quota, concurrent connections, or per-table contention.

### Architecture

Each job gets its own dedicated topic and subscription. Both jobs write to the **same** BigQuery table to reproduce the production scenario.

```
Publisher A (10k msgs/sec, ~10KB each, 100 MB/s)
    -> Topic A (perf_test_topic_a)
        -> Sub A (perf_test_sub_a)
            -> Dataflow Job A (pipeline_json.py, N workers)
                -> BQ: demo_dataset_asia.taxi_events_perf

Publisher B (same config)
    -> Topic B (perf_test_topic_b)
        -> Sub B (perf_test_sub_b)
            -> Dataflow Job B (same pipeline, same BQ table)
```

### Message Design

* **Pipeline:** `pipeline_json.py` -- stores the entire payload in a BQ `JSON` column. Minimal CPU overhead isolates the BQ write bottleneck.
* **Message size:** ~10,000 bytes JSON per message (9 taxi ride fields + `_padding` field).
* **Publish rate:** 10,000 msgs/sec per publisher = 100 MB/s.
* **BQ row size:** ~10,128 bytes (payload + 128 bytes metadata overhead).
* **BQ write throughput:** ~101 MB/s per job, ~202 MB/s for two jobs (67% of 300 MB/s regional quota).

The synthetic publisher (`scripts/synthetic_publisher.py`) supports a `--dry-run` mode to validate all size calculations before spending on GCP resources.

### Phased Test Plan

**Phase 1: Baseline (single job, 3 workers)**
1. Create resources: `./scripts/run_perf_test.sh setup`
2. Start Publisher A: `./scripts/run_perf_test.sh publisher-a`
3. Launch Job A: `./scripts/run_perf_test.sh job-a`
4. Run for 15 minutes. Record: CPU%, BQ write MB/s, Pub/Sub backlog.
5. Expected: handles 100 MB/s, CPU < 30%.

**Phase 2: Noisy neighbor (two jobs, 3 workers each)**
1. Start Publisher B: `./scripts/run_perf_test.sh publisher-b`
2. Launch Job B: `./scripts/run_perf_test.sh job-b`
3. Monitor: does Job A's throughput drop? What's the combined total?
4. Check BQ quota usage and concurrent connections.

**Phase 3: Horizontal scaling (10-20 workers)**
1. Update `NUM_WORKERS` in `run_perf_test.sh` to 10 or 20.
2. Repeat Phase 1 and Phase 2.
3. Does throughput scale linearly with workers?
    * Yes: stream count / parallelism was the Phase 1 bottleneck.
    * No: BQ quota or per-table contention is the limit.

### Decision Matrix

| Combined Throughput | BQ Quota Usage | Connections | Diagnosis | Action |
| :--- | :--- | :--- | :--- | :--- |
| ~200 MB/s | < 100% | < 1,000 | No contention | System works as expected |
| ~120 MB/s | ~100% | < 1,000 | **Throughput quota** | Request quota increase |
| ~120 MB/s | < 100% | ~1,000 | **Connection limit** | Request connection increase or use multiplexing |
| ~120 MB/s | < 100% | < 1,000 | **Per-table contention** | Write to separate tables or use different partitioning |

## 6. Implementation

### Scripts

| Script | Purpose |
| :--- | :--- |
| `scripts/synthetic_publisher.py` | High-throughput publisher with `--target_mbps`, `--message_size_bytes`, `--dry-run`. Message generation is broker-agnostic. |
| `scripts/run_perf_test.sh` | Phased orchestration: `setup`, `publisher-a/b`, `job-a/b`, `monitor`, `dry-run`. |
| `scripts/cleanup_perf_test.sh` | Idempotent teardown of all test resources. |

### Configuration

All test parameters are configurable at the top of `run_perf_test.sh`:

```bash
PROJECT_ID="your-project-id"
REGION="asia-southeast1"
BIGQUERY_DATASET="demo_dataset_asia"
NUM_WORKERS=3              # scale: 3, 10, 20
TARGET_MBPS=100            # per publisher
MESSAGE_SIZE_BYTES=10000   # per message (~10 KB)
```

### Dry Run Validation

Before spending on GCP resources, validate the math:

```bash
./scripts/run_perf_test.sh dry-run
```

This generates 10,000 sample messages and reports:
* Actual vs target message sizes
* Required publish rate (msgs/sec)
* Estimated BQ write throughput (MB/s)
* Quota usage percentages

## 7. Monitoring Guide

### Key Metrics

| Metric | Source | What It Tells You |
| :--- | :--- | :--- |
| BQ write throughput | `bigquerystorage.googleapis.com/write/append_bytes` | Actual bytes/sec written to BQ |
| Concurrent connections | `bigquerystorage.googleapis.com/write/max_active_streams` | Number of active write streams |
| Pub/Sub backlog | `pubsub.googleapis.com/subscription/num_undelivered_messages` | Source lag (growing = backpressure) |
| Worker CPU | Dataflow job monitoring tab | < 30% = I/O bound |
| BQ write wall time | Dataflow Execution Details > WriteToBigQuery | Increasing = BQ contention |

### Quota Dashboard

Check quota usage during the test:

```
https://console.cloud.google.com/iam-admin/quotas?project=PROJECT_ID&metric=bigquerystorage.googleapis.com
```

Filter by:
* `AppendBytesThroughputPerProjectRegion` -- throughput quota
* `ConcurrentWriteConnectionsPerProjectRegion` -- connection quota

### Interpreting Results

**Healthy (no contention):**
* Pub/Sub backlog: stable (near zero)
* Worker CPU: 10-30%
* BQ throughput: ~100 MB/s per job, scales linearly with jobs

**Backpressure (BQ-side bottleneck):**
* Pub/Sub backlog: growing
* Worker CPU: low (< 20%) -- workers are waiting, not computing
* Dataflow system lag: low (misleading -- Dataflow doesn't count BQ wait as "lag")
* BQ write wall time: increasing

## 8. Kafka Migration Notes

The synthetic publisher is designed to be broker-agnostic. The core functions `generate_message()` and `calculate_rates()` have zero dependency on Pub/Sub and can be reused with any message broker.

### To Swap to Kafka

1. **Publisher:** Replace `run_publisher()` with a Kafka-specific implementation using `confluent_kafka.Producer`. The `generate_message()` function returns a plain JSON string that works with any producer.

2. **Pipeline:** Replace `ReadFromPubSub` with `ReadFromKafka` in a new pipeline variant. The `WriteToBigQuery` sink configuration is identical regardless of source.

3. **Avro encoding (optional):** To reproduce the 24 MB/s Avro -> 100 MB/s JSON expansion, add Avro serialization in the publisher and deserialization in the pipeline. The `generate_message()` output can be serialized to Avro using `fastavro`.

### What Changes

| Component | Pub/Sub | Kafka |
| :--- | :--- | :--- |
| Publisher | `google.cloud.pubsub_v1.PublisherClient` | `confluent_kafka.Producer` |
| Pipeline source | `ReadFromPubSub(subscription=...)` | `ReadFromKafka(consumer_config=..., topics=...)` |
| Message format | JSON string (UTF-8 bytes) | JSON string or Avro binary |
| BQ write | Unchanged | Unchanged |
| Test scripts | `run_perf_test.sh` | New `run_perf_test_kafka.sh` |

### What Stays the Same

* `generate_message()` and `calculate_rates()` -- reusable as-is
* `WriteToBigQuery` configuration -- identical sink
* `cleanup_perf_test.sh` -- BQ and Dataflow cleanup is the same
* All monitoring metrics and quota limits

---
**Conclusion:**
*"We need to determine whether we are throughput-quota-bound, connection-bound, or table-contention-bound at the BigQuery sink. The test infrastructure supports scaling workers from 3 to 20 and running 1-N concurrent jobs against the same table to isolate the bottleneck. Once identified, the fix is either a quota increase request, connection multiplexing, or table-level sharding."*

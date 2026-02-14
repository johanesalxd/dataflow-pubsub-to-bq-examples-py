# BigQuery Write Contention Test Methodology

Reproducing and diagnosing the "Noisy Neighbor" throughput degradation when multiple Dataflow jobs write to the same BigQuery table.

## 1. Problem Statement

Consider a streaming architecture with multiple Kafka-to-BQ Dataflow jobs (Avro, 24 MB/s per topic, ~100 MB/s after decode). A single job writes to BigQuery at 100 MB/s. Adding a second identical job to the same table should give 200 MB/s total, but only 120 MB/s is observed. Both jobs show low CPU and low Dataflow lag -- no errors are visible.

**Question:** Is this BQ per-table contention, quota limit, or connection limit?

## 2. Hypothesis

BQ Storage Write API has per-table throughput limits beyond the documented project-level quotas (300 MB/s regional, 1,000 connections). When multiple Dataflow jobs write to the same table, they contend for per-table write bandwidth, causing throughput degradation invisible to standard Dataflow metrics.

**Resolution:** This hypothesis was disproven across four rounds of testing. Rounds 1-2 (Python SDK, 10 KB messages) confirmed no per-table contention -- 2 jobs scale linearly to ~330 MB/s. Round 3 (Python SDK, 500-byte messages) showed the Python SDK is CPU-bound at ~55k rows/sec. Round 4 (Java SDK, 500-byte messages) was the definitive test: using the same technology stack as the production workload, 2 Java jobs achieved **757k rows/sec combined** (~425 MB/s) with **zero per-job degradation** and only **25 concurrent connections**. The bottleneck is **upstream** of Dataflow and BigQuery -- specifically in the Kafka source path (consumer configuration, Avro deserialization, or dynamic routing overhead). See [Round 1](perf_test_results_round1.md), [Round 2](perf_test_results_round2.md), [Round 3](perf_test_results_round3.md), and [Round 4](perf_test_results_round4.md).

## 3. BigQuery Storage Write API Quotas

Source: [BigQuery Quotas -- Storage Write API](https://cloud.google.com/bigquery/quotas#write-api-limits)

| Resource | Multi-Region (`us`/`eu`) | Regional (e.g., `asia-southeast1`) |
| :--- | :--- | :--- |
| **Throughput** | 3 GB/s per project | 300 MB/s per project |
| **Concurrent Connections** | 10,000 per project | 1,000 per project |
| **CreateWriteStream** | 10,000 streams/hour | 10,000 streams/hour |

Key details:
- Throughput quota is metered on the **destination project** (where the BQ dataset lives).
- Concurrent connections are metered on the **client project** (the Dataflow project).
- When the BQ sink is saturated, Dataflow workers **self-throttle** and stop pulling from Pub/Sub. This causes low CPU and low Dataflow lag, while the Pub/Sub backlog grows.
- Each Beam worker may open 10-50 write streams. Two jobs with 20 workers each could approach the 1,000 regional connection limit.

## 4. Test Architecture

A single Dataflow batch publisher pushes 36M messages to one Pub/Sub topic. Multiple subscriptions on that topic each feed a separate consumer Dataflow job. All consumer jobs write to the same BigQuery table.

```
                         ┌→ Sub A → Dataflow Job A ─┐
Publisher (Dataflow)     │                           │
  → perf_test_topic ─────┤                           ├→ BQ: taxi_events_perf
                         │                           │
                         └→ Sub B → Dataflow Job B ─┘
                        (→ Sub N → Dataflow Job N)
```

### Why This Design

- **Single topic, multiple subscriptions:** Each subscription independently receives all messages. One publisher, identical data for every consumer. Fair comparison.
- **Batch publisher on Dataflow:** Scalable, no local machine bottleneck. Auto-terminates after publishing 36M messages.
- **`pipeline_json.py`:** Stores the entire payload in a BQ `JSON` column. Minimal CPU overhead isolates the BQ write bottleneck.
- **Same BQ table:** Reproduces the production scenario where jobs compete for the same table.

### Write Method

Rounds 1-3 use the Python consumer pipeline (`pipeline_json.py`) with `STORAGE_WRITE_API` (exactly-once semantics, application-created streams). Round 4 uses the Java consumer pipeline (`PubSubToBigQueryJson.java`) with `STORAGE_API_AT_LEAST_ONCE` (default stream) and `useStorageApiConnectionPool=true` (multiplexing), matching the production workload configuration.

The switch to at-least-once with connection pooling in round 4 reduced concurrent connections from ~60 per job (rounds 1-2) to ~10-15 per job, confirming multiplexing effectiveness. Round 3 showed that write method has no measurable impact on throughput when the pipeline is CPU-bound.

### Message Design

| Parameter | Rounds 1-2 | Rounds 3-4 |
| :--- | :--- | :--- |
| Message size | ~10,000 bytes (9 taxi fields + `_padding`) | ~500 bytes (9 taxi fields, no padding) |
| Total messages | 36,000,000 (per subscription) | 400,000,000 (per subscription) |
| Total data | ~360 GB (per subscription) | ~200 GB (per subscription) |
| Duration at 100 MB/s | ~60 minutes | ~35 minutes |

## 5. Test Procedure

### 5.1 Prerequisites

Required GCP APIs: Dataflow, Pub/Sub, BigQuery, Cloud Storage.

The `PROJECT_ID` is configured at the top of:
- `scripts/run_perf_test.sh`
- `scripts/cleanup_perf_test.sh`

### 5.2 Validate Sizes (Dry Run)

No GCP resources are created, no cost incurred.

```bash
./scripts/run_perf_test.sh dry-run
```

### 5.3 Setup Resources

Creates GCS bucket, BQ table, Pub/Sub topic, N subscriptions, and builds the wheel. Idempotent -- safe to re-run.

```bash
./scripts/run_perf_test.sh setup
```

### 5.4 Publish Messages

Launches a batch Dataflow job that generates 36M synthetic messages and publishes them to the topic. Each subscription receives a full copy. The job auto-terminates when done.

```bash
# Launch publisher
./scripts/run_perf_test.sh publish

# Check status (wait for JOB_STATE_DONE)
./scripts/run_perf_test.sh publish-status
```

**Important:** All subscriptions must exist before publishing. If you need more consumers, increase `NUM_CONSUMERS` in `run_perf_test.sh` and re-run `setup` before publishing.

After the publisher finishes, verify each subscription received the expected data volume (~360 GB):

```bash
curl -s -H "Authorization: Bearer $(gcloud auth print-access-token)" \
  "https://monitoring.googleapis.com/v3/projects/PROJECT_ID/timeSeries?\
filter=metric.type%3D%22pubsub.googleapis.com%2Fsubscription%2Fbacklog_bytes%22\
%20AND%20(resource.labels.subscription_id%3D%22perf_test_sub_a%22\
%20OR%20resource.labels.subscription_id%3D%22perf_test_sub_b%22)\
&interval.startTime=$(date -u -d '30 minutes ago' +%Y-%m-%dT%H:%M:%SZ)\
&interval.endTime=$(date -u +%Y-%m-%dT%H:%M:%SZ)\
&aggregation.alignmentPeriod=60s\
&aggregation.perSeriesAligner=ALIGN_MEAN"
```

Each subscription should show ~360,000,000,000 bytes (~360 GB). If significantly less, the publisher may have failed partway through -- check the Dataflow job logs.

### 5.5 Phase 1 -- Single Job Baseline

Launch only Job A. Wait 10 minutes for steady state.

```bash
./scripts/run_perf_test.sh job a
```

Record:
- Worker CPU %
- BQ write throughput
- Pub/Sub backlog for `perf_test_sub_a`

### 5.6 Phase 2 -- Noisy Neighbor

Keep Job A running. Launch Job B.

```bash
./scripts/run_perf_test.sh job b
```

Wait 10+ minutes. Observe whether Job A's metrics degrade after Job B starts writing to the same table.

### 5.7 Scaling Variations

To add more consumers or change worker count:

```bash
# Edit configuration in scripts/run_perf_test.sh:
#   NUM_CONSUMERS=4        (add more subscriptions)
#   CONSUMER_NUM_WORKERS=10 (more workers per job)

# Re-run setup (idempotent, creates new subs without touching existing ones)
./scripts/run_perf_test.sh setup

# Re-publish (new subs need fresh messages)
./scripts/run_perf_test.sh publish

# Launch additional jobs
./scripts/run_perf_test.sh job c
./scripts/run_perf_test.sh job d
```

### 5.8 Cleanup

Cancels all running Dataflow jobs, deletes subscriptions, topic, and BQ tables.

```bash
./scripts/cleanup_perf_test.sh         # interactive
./scripts/cleanup_perf_test.sh --force  # skip confirmation
```

Consumer jobs are streaming and will idle indefinitely after draining their backlog. Always run cleanup when done.

## 6. Monitoring

### Console Links

Run `./scripts/run_perf_test.sh monitor` to print all URLs. Key pages:

| Page | What to Check |
| :--- | :--- |
| [Dataflow Jobs](https://console.cloud.google.com/dataflow/jobs) | Job state, worker count, system lag |
| [Pub/Sub Subscriptions](https://console.cloud.google.com/cloudpubsub/subscription/list) | Unacked message count (backlog) |
| [BQ Quota (throughput)](https://console.cloud.google.com/iam-admin/quotas?metric=bigquerystorage.googleapis.com/write/append_bytes) | AppendBytesThroughputPerProjectRegion |
| [BQ Quota (connections)](https://console.cloud.google.com/iam-admin/quotas?metric=bigquerystorage.googleapis.com/write/max_active_streams) | ConcurrentWriteConnectionsPerProjectRegion |

### Cloud Monitoring Metrics

Open [Metrics Explorer](https://console.cloud.google.com/monitoring/metrics-explorer) and search for:

| Metric | Aggregation | What It Shows |
| :--- | :--- | :--- |
| `bigquerystorage.googleapis.com/write/uploaded_bytes_count` | Sum, 1 min | BQ write throughput (bytes/sec) |
| `bigquerystorage.googleapis.com/dataflow_write/uploaded_bytes_count` | Sum, 1 min | Same, Dataflow-specific view |
| `bigquerystorage.googleapis.com/write/concurrent_connections` | Sum, 1 min | Active write streams |
| `pubsub.googleapis.com/subscription/num_unacked_messages_by_region` | -- | Subscription backlog |

### Monitoring API Queries

These `curl` commands pull metrics programmatically. Replace `PROJECT_ID` with your project.

**Pub/Sub backlog (both subscriptions, last 30 minutes):**

```bash
curl -s -H "Authorization: Bearer $(gcloud auth print-access-token)" \
  "https://monitoring.googleapis.com/v3/projects/PROJECT_ID/timeSeries?\
filter=metric.type%3D%22pubsub.googleapis.com%2Fsubscription%2Fnum_unacked_messages_by_region%22\
%20AND%20(resource.labels.subscription_id%3D%22perf_test_sub_a%22\
%20OR%20resource.labels.subscription_id%3D%22perf_test_sub_b%22)\
&interval.startTime=$(date -u -d '30 minutes ago' +%Y-%m-%dT%H:%M:%SZ)\
&interval.endTime=$(date -u +%Y-%m-%dT%H:%M:%SZ)\
&aggregation.alignmentPeriod=60s\
&aggregation.perSeriesAligner=ALIGN_MEAN"
```

**Pub/Sub backlog bytes (data volume per subscription, last 30 minutes):**

```bash
curl -s -H "Authorization: Bearer $(gcloud auth print-access-token)" \
  "https://monitoring.googleapis.com/v3/projects/PROJECT_ID/timeSeries?\
filter=metric.type%3D%22pubsub.googleapis.com%2Fsubscription%2Fbacklog_bytes%22\
%20AND%20(resource.labels.subscription_id%3D%22perf_test_sub_a%22\
%20OR%20resource.labels.subscription_id%3D%22perf_test_sub_b%22)\
&interval.startTime=$(date -u -d '30 minutes ago' +%Y-%m-%dT%H:%M:%SZ)\
&interval.endTime=$(date -u +%Y-%m-%dT%H:%M:%SZ)\
&aggregation.alignmentPeriod=60s\
&aggregation.perSeriesAligner=ALIGN_MEAN"
```

**BQ write throughput (last 30 minutes):**

```bash
curl -s -H "Authorization: Bearer $(gcloud auth print-access-token)" \
  "https://monitoring.googleapis.com/v3/projects/PROJECT_ID/timeSeries?\
filter=metric.type%3D%22bigquerystorage.googleapis.com%2Fwrite%2Fuploaded_bytes_count%22\
%20AND%20resource.labels.location%3D%22asia-southeast1%22\
&interval.startTime=$(date -u -d '30 minutes ago' +%Y-%m-%dT%H:%M:%SZ)\
&interval.endTime=$(date -u +%Y-%m-%dT%H:%M:%SZ)\
&aggregation.alignmentPeriod=60s\
&aggregation.perSeriesAligner=ALIGN_RATE"
```

**BQ concurrent connections (last 30 minutes):**

```bash
curl -s -H "Authorization: Bearer $(gcloud auth print-access-token)" \
  "https://monitoring.googleapis.com/v3/projects/PROJECT_ID/timeSeries?\
filter=metric.type%3D%22bigquerystorage.googleapis.com%2Fwrite%2Fconcurrent_connections%22\
&interval.startTime=$(date -u -d '30 minutes ago' +%Y-%m-%dT%H:%M:%SZ)\
&interval.endTime=$(date -u +%Y-%m-%dT%H:%M:%SZ)\
&aggregation.alignmentPeriod=60s\
&aggregation.perSeriesAligner=ALIGN_MEAN"
```

**BQ write timeline (per-table breakdown, BigQuery SQL):**

```sql
SELECT
  TIMESTAMP_TRUNC(start_timestamp, MINUTE) AS minute,
  SUM(total_input_bytes) AS bytes_written,
  ROUND(SUM(total_input_bytes) / 1000000, 1) AS mb_written,
  SUM(total_rows) AS rows_written
FROM
  `region-asia-southeast1`.INFORMATION_SCHEMA.WRITE_API_TIMELINE
WHERE
  table_id = 'taxi_events_perf'
  AND start_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
GROUP BY minute
ORDER BY minute;
```

## 7. Decision Matrix

| Combined Throughput | BQ Quota Usage | Connections | Diagnosis | Recommendation |
| :--- | :--- | :--- | :--- | :--- |
| ~200 MB/s (linear) | < 100% | < 1,000 | No contention | System works as expected |
| ~120 MB/s | ~100% | < 1,000 | **Throughput quota** | Request quota increase |
| ~120 MB/s | < 100% | ~1,000 | **Connection limit** | Use `STORAGE_API_AT_LEAST_ONCE` (default stream), request connection quota increase, or enable multiplexing (Java/Go only) |
| ~120 MB/s | < 100% | < 1,000 | **Per-table contention** | Write to separate tables, change partitioning, or use multi-region |

## 8. Cost Estimate

### Per test run (36M messages, 2 consumers)

| Component | Cost |
| :--- | :--- |
| Pub/Sub publish (360 GB x 1) | ~$14.06 |
| Pub/Sub subscribe (360 GB x 2 subs) | ~$28.13 |
| Dataflow publisher (5 workers, ~10 min) | ~$0.33 |
| Dataflow consumer A (3 workers, ~60 min) | ~$1.20 |
| Dataflow consumer B (3 workers, ~60 min) | ~$1.20 |
| **Total** | **~$44.92** |

Each additional consumer adds ~$15 (Pub/Sub delivery + Dataflow workers).

### Pricing references

- Pub/Sub: $40/TiB (first 10 TiB/month) for both publish and subscribe
- Dataflow Streaming Engine: ~$0.069/vCPU-hr + $0.003557/GB-hr + $0.018/vCPU-hr (SE)
- BQ Storage Write API: included in BQ pricing (no separate charge for writes)

## 9. Reproducing the Test

### Quick start

```bash
# 1. Setup
./scripts/run_perf_test.sh setup

# 2. Publish (batch, auto-terminates)
./scripts/run_perf_test.sh publish
./scripts/run_perf_test.sh publish-status  # wait for JOB_STATE_DONE

# 3. Phase 1 -- single job baseline
./scripts/run_perf_test.sh job a
# Wait 10 minutes

# 4. Phase 2 -- noisy neighbor
./scripts/run_perf_test.sh job b
# Wait 10+ minutes, observe metrics

# 5. Monitor
./scripts/run_perf_test.sh monitor

# 6. Cleanup
./scripts/cleanup_perf_test.sh --force
```

### Configurable parameters

Edit the configuration section at the top of `scripts/run_perf_test.sh`:

| Parameter | Default | Description |
| :--- | :--- | :--- |
| `NUM_CONSUMERS` | `2` | Number of consumer subscriptions (a, b, c, ...) |
| `CONSUMER_NUM_WORKERS` | `3` | Workers per consumer job |
| `CONSUMER_MACHINE_TYPE` | `n2-standard-4` | Consumer worker machine type |
| `PUBLISHER_NUM_WORKERS` | `5` | Workers for the publisher job |
| `PUBLISHER_MACHINE_TYPE` | `n2-standard-4` | Publisher worker machine type |
| `NUM_MESSAGES` | `36000000` | Total messages (36M = ~360 GB = ~1 hr at 100 MB/s) |
| `MESSAGE_SIZE_BYTES` | `10000` | Target size per message in bytes |

### Scaling test rounds

| Round | SDK | Msg Size | Workers/Job | Consumers | Purpose | Status |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 1 | Python | 10 KB | 3x n2-std-4 | 2 | Baseline -- per-table contention | Done |
| 2 | Python | 10 KB | 3x n2-std-4 | 3 | Throughput quota enforcement | Done |
| 3 | Python | 500 B | 3x n2-std-4 | 1 | Element rate + write method comparison | Done |
| 4 | **Java** | 500 B | 3x n2-highcpu-8 | 2 | Java SDK at production scale + 2-job scaling | Done |

**All four rounds complete. Dataflow and BigQuery are definitively cleared as the bottleneck.**

- Rounds 1-2 proved no per-table contention and identified quota enforcement above ~314 MB/s.
- Round 3 showed Python SDK is CPU-bound at high element rates; write method is irrelevant.
- Round 4 used the same Java SDK, write method, and connection pooling as the production workload: 2 jobs achieved **757k rows/sec** (~425 MB/s) with **zero degradation** and only **25 connections**. The production bottleneck is upstream (Kafka source path).

See [Round 1](perf_test_results_round1.md), [Round 2](perf_test_results_round2.md), [Round 3](perf_test_results_round3.md), and [Round 4](perf_test_results_round4.md).

## 10. Kafka Migration

To reproduce this test with Kafka instead of Pub/Sub:

| Component | Pub/Sub (current) | Kafka (swap) |
| :--- | :--- | :--- |
| Publisher | Dataflow batch → `WriteToPubSub` | Dataflow batch → `WriteToKafka` |
| Pipeline source | `ReadFromPubSub(subscription=...)` | `ReadFromKafka(consumer_config=..., topics=...)` |
| Message format | JSON string | JSON string or Avro binary |
| BQ write | Unchanged | Unchanged |

To add Avro encoding (reproducing a scenario where 24 MB/s of Avro data expands to ~100 MB/s after JSON decode), serialize the `generate_message()` output with `fastavro` in the publisher and add deserialization in the pipeline.

## 11. Conclusion

The original question -- "Is this BQ per-table contention, quota limit, or connection limit?" -- is answered:

**None of the above. Dataflow and BigQuery are not the bottleneck.**

| Hypothesis | Result |
| :--- | :--- |
| Per-table contention | Disproven. 2 jobs writing to the same table scale linearly with no per-job degradation (Rounds 1, 4). |
| Connection limit | Not reached. Peak 25 connections with pooling (2.5% of 1,000 limit, Round 4). Without pooling: 180 connections (18%, Round 2). |
| Throughput quota | Only triggered above ~314 MB/s via sawtooth throttling (Round 2). Production workload (~200 MB/s) is well below this limit. |
| Write method | No effect. Exactly-once and at-least-once produce identical throughput (Round 3). |
| Element rate | Not a BQ limitation. 757k rows/sec sustained with zero throttling (Round 4). |

### Key Results Across All Rounds

| Round | SDK | Single Job | 2 Jobs Combined | Per-Job Degradation | Connections |
| :--- | :--- | :--- | :--- | :--- | :--- |
| 1 | Python | 160 MB/s, 16k RPS | 330 MB/s, 33k RPS | None | 120 |
| 2 | Python | 161 MB/s, 16k RPS | 328 MB/s (2-job tail) | None | 120 |
| 3 | Python | 31 MB/s, 55k RPS | N/A (CPU-bound) | N/A | N/A |
| **4** | **Java** | **190 MB/s, 340k RPS** | **393 MB/s, 700k RPS** | **None** | **25** |

Round 4 is the definitive test. Using the **same Java SDK, same write method (`STORAGE_API_AT_LEAST_ONCE`), same connection pooling, same region (`asia-southeast1`), and same message size (~500 bytes)** as the production workload, 2 Dataflow jobs achieved 757k rows/sec combined -- **3.8x the production target of 200k rows/sec** -- with zero per-job degradation. The production scenario (single job at 200k rows/sec dropping to 100k when a second job starts) cannot be reproduced when the pipeline reads from Pub/Sub instead of Kafka.

### Secondary Findings

1. **Quota enforcement mechanism (Round 2).** When combined demand exceeds ~314 MB/s, BigQuery enforces the regional throughput quota via periodic sawtooth throttling (burst at ~550 MB/s for 5-7 min, throttle to ~20 MB/s for 2-4 min). This is relevant for future scaling beyond 500k RPS but does not explain the current production issue.

2. **Connection pooling effectiveness (Round 4).** Enabling `useStorageApiConnectionPool=true` reduced connections from ~60 per job (without pooling) to ~10-15 per job (with pooling) -- a ~5x reduction. This is essential for pipelines with dynamic multi-table routing.

3. **Python vs Java SDK performance (Round 3 vs 4).** Java achieves ~6x the rows/sec of Python at the same message size (~340k vs ~55k rows/sec), but uses 2x the vCPUs (24 vs 12), yielding ~3x better per-vCPU efficiency. Both SDKs are CPU-near-capacity at these element rates: Python at 100% on 12 vCPUs, Java at 75-90% on 24 vCPUs. The difference reflects JIT compilation, true multi-threading, and native protobuf serialization.

### Recommendations

1. **Investigate the Kafka source path.** The production bottleneck is isolated to the source side of the pipeline. Switching from Kafka to Pub/Sub eliminates the degradation entirely. Key areas to investigate:
   - **Kafka consumer group configuration.** If both Dataflow jobs share the same consumer group, Kafka splits partitions between them -- each job gets half the data, not a copy. This exactly explains the 200k → 100k per-job pattern. Verify each job uses a **separate consumer group** to receive full data.
   - **Kafka consumer settings.** `fetch.max.bytes`, `max.partition.fetch.bytes`, and `max.poll.records` directly control consumer throughput.
   - **Network throughput** between the Kafka cluster and Dataflow workers.
   - **Avro deserialization overhead** for complex schemas.

2. **Verify Kafka partition assignment.** Monitor `dataflow.googleapis.com/job/elements_produced` on the ReadFromKafka step to confirm whether each job reads the same total volume (independent consumer groups) or half the volume (shared consumer group) when 2 jobs run concurrently.

3. **Test with a minimal Kafka-to-BQ pipeline.** Read from Kafka, write raw bytes to a single BQ table (no Avro decode, no dynamic routing) to isolate whether the bottleneck is in the read path or the transform/routing logic.

4. **Monitor quota usage** with alerts on `bigquerystorage.googleapis.com/write/append_bytes_region` as a best practice for capacity planning.

5. **Quota increase is not needed** at current production volumes. The regional quota of ~314 MB/s accommodates the 500k RPS target (at ~500 bytes/row = ~250 MB/s). A quota increase would only be needed if row sizes grow or throughput targets exceed ~550k RPS.

## References

- [BigQuery Storage Write API best practices](https://cloud.google.com/bigquery/docs/write-api-best-practices)
- [Pub/Sub to BigQuery best practices](https://cloud.google.com/dataflow/docs/guides/pubsub-bigquery-best-practices)
- [Pub/Sub to BigQuery performance benchmarks](https://cloud.google.com/dataflow/docs/guides/pubsub-bigquery-performance)
- [Write from Dataflow to BigQuery](https://cloud.google.com/dataflow/docs/guides/write-to-bigquery)
- [BigQuery Storage Write API quotas](https://cloud.google.com/bigquery/quotas#write-api-limits)

# BigQuery Write Contention Test Methodology

Reproducing and diagnosing the "Noisy Neighbor" throughput degradation when multiple Dataflow jobs write to the same BigQuery table.

## 1. Problem Statement

Consider a streaming architecture with multiple Kafka-to-BQ Dataflow jobs (Avro, 24 MB/s per topic, ~100 MB/s after decode). A single job writes to BigQuery at 100 MB/s. Adding a second identical job to the same table should give 200 MB/s total, but only 120 MB/s is observed. Both jobs show low CPU and low Dataflow lag -- no errors are visible.

**Question:** Is this BQ per-table contention, quota limit, or connection limit?

## 2. Hypothesis

BQ Storage Write API has per-table throughput limits beyond the documented project-level quotas (300 MB/s regional, 1,000 connections). When multiple Dataflow jobs write to the same table, they contend for per-table write bandwidth, causing throughput degradation invisible to standard Dataflow metrics.

**Resolution:** This hypothesis was disproven. Testing confirmed there is **no per-table contention** -- Dataflow and BigQuery scale linearly to the regional throughput quota (~314.6 MB/s). Two jobs writing to the same table achieved ~330 MB/s combined with zero per-job degradation. Since the production workload (~200 MB/s combined) is well below the quota, the bottleneck is **upstream** of Dataflow and BigQuery (e.g., Kafka consumer configuration, Avro deserialization, or pipeline transform logic). See [Round 1 results](perf_test_results_round1.md) and [Round 2 results](perf_test_results_round2.md).

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

The consumer pipeline (`pipeline_json.py`) uses `STORAGE_WRITE_API` (exactly-once semantics). This creates application-created streams -- each worker opens multiple dedicated write connections to BigQuery. The number of streams per worker is determined by Beam's auto-sharding and is not explicitly configured.

**(Optional)** A follow-up test with `STORAGE_API_AT_LEAST_ONCE` (default stream, shared connections) could isolate whether the stream type affects throughput per connection. With the default stream, all workers append to a single shared stream, resulting in significantly fewer connections. Since testing confirmed Dataflow and BigQuery are not the bottleneck, this test is not required to resolve the original problem.

### Message Design

| Parameter | Value |
| :--- | :--- |
| Message size | ~10,000 bytes (9 taxi ride fields + `_padding`) |
| Total messages | 36,000,000 (per subscription) |
| Total data | ~360 GB (per subscription) |
| Duration at 100 MB/s | ~60 minutes |

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

| Round | Workers/Job | Consumers | Purpose | Status |
| :--- | :--- | :--- | :--- | :--- |
| 1 | 3 | 2 | Baseline -- find single-job ceiling | Done |
| 2 | 3 | 3 | Does 300 MB/s quota cap combined throughput? | Done |
| 3 | 3 | 4+ | Further scaling if round 2 doesn't cap | Optional |
| 4 | 10+ | 2 | Push toward connection limit | Optional |

**Dataflow and BigQuery cleared in rounds 1-2.** Round 1 proved no per-table contention exists (2 jobs scale linearly to ~330 MB/s). Round 2 confirmed quota enforcement exists above ~314 MB/s via sawtooth throttling, but the production workload (~200 MB/s) is well below this threshold. The bottleneck is upstream. Rounds 3-4 are optional. See [Round 1 results](perf_test_results_round1.md) and [Round 2 results](perf_test_results_round2.md).

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
| Per-table contention | Disproven. 2 jobs writing to the same table scale linearly to ~330 MB/s with no per-job degradation (Round 1). |
| Connection limit | Not reached. 180 connections at peak (18% of 1,000 limit). Not a factor at current scale (Round 2). |
| Throughput quota | Only triggered above ~314 MB/s. The production workload (~200 MB/s) is well below this limit (Round 1 proved 2 jobs at ~330 MB/s with no throttling). |

The production scenario (2 jobs at ~100 MB/s each, ~120 MB/s combined instead of ~200 MB/s) cannot be explained by any BigQuery or Dataflow limitation. This test demonstrated that a single Dataflow job achieves ~160 MB/s and 2 jobs scale linearly to ~330 MB/s. The production jobs achieving only ~100 MB/s each indicates the bottleneck is **upstream** of Dataflow and BigQuery.

### Secondary Finding: Quota Enforcement Mechanism

Round 2 discovered that when combined demand exceeds ~314 MB/s, BigQuery enforces the regional throughput quota via periodic sawtooth throttling (burst at ~550 MB/s for 5-7 min, then throttle to ~20 MB/s for 2-4 min). This is relevant for future scaling but does not explain the current production issue.

### Recommendations

1. **Investigate the upstream bottleneck.** Each Dataflow job should achieve ~160 MB/s based on this test. The production jobs only achieve ~100 MB/s -- the gap points to Kafka consumer configuration, Avro deserialization overhead, pipeline transform logic, or network constraints.
2. **Profile the production pipeline** to identify where the ~60 MB/s per-job gap is lost compared to the baseline established in this test.
3. **Monitor quota usage** with alerts on `bigquerystorage.googleapis.com/write/append_bytes_region` as a best practice.
4. **Quota increase or multi-region** is only needed if future scaling pushes combined throughput past ~314 MB/s.

## References

- [BigQuery Storage Write API best practices](https://cloud.google.com/bigquery/docs/write-api-best-practices)
- [Pub/Sub to BigQuery best practices](https://cloud.google.com/dataflow/docs/guides/pubsub-bigquery-best-practices)
- [Pub/Sub to BigQuery performance benchmarks](https://cloud.google.com/dataflow/docs/guides/pubsub-bigquery-performance)
- [Write from Dataflow to BigQuery](https://cloud.google.com/dataflow/docs/guides/write-to-bigquery)
- [BigQuery Storage Write API quotas](https://cloud.google.com/bigquery/quotas#write-api-limits)

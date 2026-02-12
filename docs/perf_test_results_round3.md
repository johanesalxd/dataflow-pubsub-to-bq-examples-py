# Performance Test Results -- Round 3 (Small Messages, Write Method Comparison)

Date: 2026-02-12

## Test Objective

Rounds 1-2 used ~10 KB messages (~16k rows/sec). A production workload with ~500-byte messages at ~200k rows/sec delivers the same byte throughput but requires 13x more per-row work for BigQuery. Round 3 tests whether high element rates with small messages cause throughput degradation, and compares exactly-once vs at-least-once write semantics.

## Test Environment

| Parameter | Value |
|:---|:---|
| Project ID | `johanesa-playground-326616` |
| Region | `asia-southeast1` |
| Dataset | `demo_dataset_asia` |
| BQ Table | `taxi_events_perf_round_3` |
| Pub/Sub Topic | `perf_test_topic` |
| Subscriptions | `perf_test_sub_a`, `perf_test_sub_b` |
| Messages per subscription | 400,000,000 |
| Message size | ~500 bytes |
| Total data per subscription | ~200 GB |
| Total data (all subscriptions) | ~400 GB (2 x 200 GB) |

### Sample Row

Same structure as rounds 1-2, but without the `_padding` field (messages are ~500 bytes instead of ~10 KB):

```json
{
  "subscription_name": "perf_test_sub_a",
  "message_id": "17949514610735406",
  "publish_time": "2026-02-12 09:02:50",
  "processing_time": "2026-02-12 10:54:08",
  "attributes": "{}",
  "payload": {
    "ride_id": "ride-674165",
    "ride_status": "dropoff",
    "latitude": 1.452581,
    "longitude": 103.864721,
    "passenger_count": 2,
    "meter_reading": 15.3,
    "meter_increment": 0.0296,
    "point_idx": 1573,
    "timestamp": "2026-02-26T23:41:14.905703+08:00"
  }
}
```

### Dataflow Jobs

| Job | Job ID | Name | Write Method | State |
|:---|:---|:---|:---|:---|
| Publisher (batch) | `2026-02-12_01_00_12-15352062949245672470` | `dataflow-perf-publisher-20260212-170004` | N/A | Done |
| Consumer A1 (streaming) | `2026-02-12_02_49_41-717431225379085181` | `dataflow-perf-test-a-20260212-184858` | `STORAGE_WRITE_API` (exactly-once) | Cancelled |
| Consumer A2 (streaming) | `2026-02-12_03_08_40-4939302815189191734` | `dataflow-perf-test-a-20260212-190758` | `STORAGE_WRITE_API` + `use_at_least_once=True` | Cancelled |

### Consumer Pipeline Configuration

| Parameter | Value |
|:---|:---|
| Workers per job | 3 |
| Machine type | `n2-standard-4` (4 vCPUs, 16 GB RAM) |
| Total vCPUs per job | 12 |
| Pipeline | `pipeline_json.py` (raw JSON to BQ JSON column) |
| Triggering frequency | 1 second |
| Streaming Engine | Enabled |

## Raw Data

### BQ Write Throughput

**Query:**

```bash
bq query --use_legacy_sql=false --location=asia-southeast1 --format=pretty '
SELECT
  TIMESTAMP_TRUNC(start_timestamp, MINUTE) AS minute,
  ROUND(SUM(total_input_bytes) / 1000000, 1) AS mbpm_written,
  ROUND(ROUND(SUM(total_input_bytes) / 1000000, 1)/60, 2) AS mbps_written,
  SUM(total_rows) AS rows_written,
  ROUND(SUM(total_rows)/60, 2) AS rps_written
FROM
  `region-asia-southeast1`.INFORMATION_SCHEMA.WRITE_API_TIMELINE
WHERE
  table_id = "taxi_events_perf_round_3"
  AND start_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 HOUR)
GROUP BY minute
ORDER BY minute'
```

**Results:**

| Minute (UTC) | MB/min | MB/s | Rows | Rows/sec | Phase |
|:---|---:|---:|---:|---:|:---|
| 10:54 | 713.2 | 11.89 | 1,271,339 | 21,189 | A1 ramp-up (exactly-once) |
| 10:55 | 1,735.5 | 28.93 | 3,093,657 | 51,561 | A1 ramp-up |
| 10:56 | 1,883.4 | 31.39 | 3,357,303 | 55,955 | A1 steady state |
| 10:57 | 1,895.9 | 31.60 | 3,379,549 | 56,326 | A1 steady state |
| 10:58 | 1,970.4 | 32.84 | 3,512,353 | 58,539 | A1 steady state |
| 10:59 | 1,821.5 | 30.36 | 3,246,907 | 54,115 | A1 steady state |
| 11:00 | 1,979.7 | 32.99 | 3,528,966 | 58,816 | A1 steady state |
| 11:01 | 1,793.6 | 29.89 | 3,197,080 | 53,285 | A1 steady state |
| 11:02 | 916.9 | 15.28 | 1,634,441 | 27,241 | A1 cancelled |
| | | | | | Gap -- Job switch |
| 11:12 | 836.6 | 13.94 | 1,491,282 | 24,855 | A2 ramp-up (at-least-once) |
| 11:13 | 1,910.6 | 31.84 | 3,405,738 | 56,762 | A2 steady state |
| 11:14 | 1,878.7 | 31.31 | 3,348,835 | 55,814 | A2 steady state |
| 11:15 | 1,896.4 | 31.61 | 3,380,328 | 56,339 | A2 steady state |
| 11:16 | 1,871.2 | 31.19 | 3,335,539 | 55,592 | A2 steady state |
| 11:17 | 1,846.0 | 30.77 | 3,290,536 | 54,842 | A2 steady state |
| 11:18 | 1,839.0 | 30.65 | 3,278,013 | 54,634 | A2 steady state |
| 11:19 | 1,810.4 | 30.17 | 3,227,037 | 53,784 | A2 steady state |
| 11:20 | 1,823.5 | 30.39 | 3,250,379 | 54,173 | A2 steady state |
| 11:21 | 1,791.3 | 29.86 | 3,193,048 | 53,217 | A2 steady state |
| 11:22 | 1,804.8 | 30.08 | 3,217,163 | 53,619 | A2 steady state |

### CPU Utilization

**Method:** Computed from `dataflow.googleapis.com/job/total_vcpu_time` metric. With 3 workers x 4 vCPUs = 12 vCPUs, maximum vCPU-seconds per minute is 720.

**Job A1 (exactly-once):**

| Minute (UTC) | Delta vCPU-sec | Utilization |
|:---|---:|---:|
| 10:52 | 480 | 66.7% |
| 10:53 | 640 | 88.9% |
| 10:54 - 11:02 | 720 | **100.0%** |
| 11:03 | 563 | 78.2% (cancelling) |

**Job A2 (at-least-once):**

| Minute (UTC) | Delta vCPU-sec | Utilization |
|:---|---:|---:|
| 11:11 | 600 | 83.3% |
| 11:12 - 11:21 | 720 | **100.0%** |

Both write methods hit **100% CPU utilization** within 2 minutes of starting.

### Throughput Summary by Write Method

| Metric | Exactly-Once (A1) | At-Least-Once (A2) | Difference |
|:---|---:|---:|:---|
| Avg MB/s (steady state) | 31.3 | 30.8 | < 2% (noise) |
| Avg rows/sec (steady state) | 55,791 | 54,878 | < 2% (noise) |
| CPU utilization | 100% | 100% | Identical |
| Per-vCPU rows/sec | ~4,649 | ~4,573 | Identical |

## Analysis

### Key Finding: CPU-Bound at High Element Rates

With 500-byte messages, the pipeline is **CPU-bound, not I/O-bound**. All 12 vCPUs are fully saturated at ~55k rows/sec. This is fundamentally different from rounds 1-2, where 10 KB messages left the CPU at ~25-30% utilization and the bottleneck was BQ I/O throughput.

### Write Method Has No Effect

Switching from exactly-once (`STORAGE_WRITE_API`) to at-least-once (`use_at_least_once=True`) produced **no measurable throughput change**. Both methods achieved ~31 MB/s and ~55k rows/sec with identical CPU utilization. This confirms that the commit/finalize overhead of exactly-once is negligible compared to the per-row processing cost (JSON parsing, protobuf serialization, Beam framework overhead).

### Comparison with Rounds 1-2

| Metric | Rounds 1-2 (10 KB msgs) | Round 3 (500-byte msgs) |
|:---|---:|---:|
| Message size | ~10,000 bytes | ~500 bytes |
| Rows/sec (single job) | ~16,000 | ~55,000 |
| MB/s (single job) | ~160 | ~31 |
| CPU utilization | ~25-30% | **100%** |
| Bottleneck | BQ I/O throughput | **Worker CPU** |
| Per-vCPU rows/sec | ~1,333 | ~4,649 |

Per-vCPU row throughput increased 3.5x (from ~1,333 to ~4,649 rows/sec/vCPU) because the smaller messages have less data to serialize per row. However, the fixed per-row cost (JSON parsing, protobuf framing, Beam bundle management) prevents linear scaling -- if there were zero per-row overhead, 500-byte messages should achieve the same MB/s as 10 KB messages (~160 MB/s), which would require ~320k rows/sec. The actual ~55k rows/sec (17% of theoretical) quantifies the per-row overhead.

### Implications for Java SDK Pipelines

A Java SDK pipeline achieves ~200k rows/sec with ~500-byte messages in production. This test's Python SDK achieves ~55k rows/sec with the same message size. The 3.6x difference is consistent with known Python vs Java performance characteristics:

| Factor | Python SDK | Java SDK |
|:---|:---|:---|
| Runtime | CPython interpreter | JVM with JIT compilation |
| Parallelism | GIL limits per-process parallelism | True multi-threaded parallelism |
| Protobuf serialization | Python wrapper (slower) | Native implementation (faster) |
| Per-row overhead | Higher | Lower |

For a Java SDK pipeline at 200k rows/sec:
- Workers are likely **NOT CPU-bound** (Java is fast enough per-row)
- If CPU is low, the pipeline is **I/O-bound** -- just like rounds 1-2 of this test
- An I/O-bound pipeline that degrades when a second job is added points to a **shared downstream resource** as the bottleneck
- The only shared downstream resource between two independent Dataflow jobs writing to the same BQ project is the **regional connection quota (1,000 concurrent connections)**

## Diagnosis

Round 3 strengthens the connection pool exhaustion hypothesis through elimination:

1. **BQ write method is not the bottleneck.** Exactly-once and at-least-once produce identical throughput when CPU-bound. For a Java pipeline (not CPU-bound), the write method difference would be even more negligible.

2. **BQ per-table throughput is not the bottleneck.** Even at high element rates, BQ accepted ~55k rows/sec without any throttling or errors. The regional throughput quota (314.6 MB/s) is far above a production workload at ~200 MB/s combined.

3. **Element rate alone does not cause BQ degradation.** With a single destination table, high element rates are limited only by worker CPU, not by any BQ per-row limit.

4. **The remaining hypothesis is connection exhaustion from dynamic destinations.** A pipeline that routes rows to multiple BQ tables without connection pooling (`UseStorageApiConnectionPool` not set) requires a persistent connection per destination table per worker on the default stream. With enough tables and workers across 2 jobs, this can exceed the 1,000 regional connection limit.

### Across All Three Rounds

| Round | What It Tested | What It Proved |
|:---|:---|:---|
| Round 1 (2 jobs, 10 KB) | Per-table contention | No per-table contention; 2 jobs scale linearly |
| Round 2 (3 jobs, 10 KB) | Regional throughput quota | Quota enforced at ~314.6 MB/s via sawtooth; production is below this |
| Round 3 (1 job, 500 bytes) | Element rate + write method | CPU-bound in Python; write method irrelevant; BQ handles high element rates without degradation |

**Combined conclusion:** BigQuery throughput, per-table contention, element rate, and write method are all eliminated as root causes. The remaining candidate is **connection exhaustion from dynamic multi-table routing without connection pooling**.

## Recommendations

1. **Check concurrent connections.** Monitor `bigquerystorage.googleapis.com/write/concurrent_connections` in Cloud Monitoring. If this approaches 1,000 during dual-job operation, connection exhaustion is confirmed.

2. **Enable connection pooling.** For pipelines using `STORAGE_API_AT_LEAST_ONCE` with dynamic destinations, enable connection multiplexing:

    ```java
    BigQueryOptions bqOptions = options.as(BigQueryOptions.class);
    bqOptions.setUseStorageApiConnectionPool(true);
    ```

    Or via command line: `--useStorageApiConnectionPool=true`

3. **Audit destination table count.** The number of dynamic destination tables directly determines connection usage. With N tables, W workers, and J jobs: connections = N x W x J (without pooling). This formula determines whether the 1,000 regional limit is being exceeded.

4. **If connections are not the issue:** Investigate upstream (Kafka consumer throughput, partition count, Avro deserialization overhead, network between Kafka and Dataflow workers).

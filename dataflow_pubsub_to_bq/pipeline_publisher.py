"""Batch pipeline for publishing synthetic messages to Pub/Sub.

Generates synthetic taxi ride JSON messages and publishes them to a
Pub/Sub topic using Dataflow workers. Runs on Dataflow to avoid local
machine CPU and network bottlenecks.

This is a batch pipeline: it generates a fixed number of messages,
publishes them as fast as the workers allow, and auto-terminates.
Messages remain in Pub/Sub subscriptions for consumer Dataflow jobs
to process.

Supports a ``--validate_only`` mode that validates message sizes and rate
calculations without launching a pipeline or publishing anything.

Usage:
    # Validate only -- check sizes and math
    python -m dataflow_pubsub_to_bq.pipeline_publisher --validate_only

    # Publish 36M messages (~360 GB, ~1 hour at 100 MB/s consumption)
    python -m dataflow_pubsub_to_bq.pipeline_publisher \
        --runner=DataflowRunner \
        --project=my-project \
        --region=asia-southeast1 \
        --topic=projects/my-project/topics/perf_test_topic \
        --num_messages=36000000 \
        --message_size_bytes=10000 \
        --num_workers=5
"""

import json
import logging

import apache_beam as beam
from apache_beam.io.gcp import pubsub
from apache_beam.options.pipeline_options import PipelineOptions

from dataflow_pubsub_to_bq.pipeline_publisher_options import PublisherPipelineOptions
from dataflow_pubsub_to_bq.transforms.synthetic_messages import (
    _BQ_METADATA_OVERHEAD_BYTES,
    calculate_rates,
    generate_message,
    generate_message_pool,
)

# Number of seed elements to create for parallelism. Each seed generates
# a batch of messages. More seeds = better distribution across workers.
_NUM_SEEDS = 100

# Pool size for pre-generating messages within each bundle.
_POOL_SIZE = 1000


def generate_batch(
    seed: int, num_messages: int, message_size_bytes: int
) -> list[bytes]:
    """Generates a batch of encoded messages for one seed element.

    Pre-generates a pool of messages and cycles through it to produce
    the required count. This avoids per-message random generation
    overhead while still providing message diversity.

    Args:
        seed: Seed element index (unused, exists for Beam FlatMap signature).
        num_messages: Number of messages to generate in this batch.
        message_size_bytes: Target size per message in bytes.

    Returns:
        List of UTF-8 encoded message bytes.
    """
    pool_size = min(_POOL_SIZE, num_messages)
    pool = generate_message_pool(message_size_bytes, pool_size)
    result: list[bytes] = []
    for i in range(num_messages):
        result.append(pool[i % pool_size])
    return result


def run_validate_only(num_messages: int, message_size_bytes: int) -> None:
    """Validates message sizes and reports rate calculations.

    Generates sample messages, measures actual sizes, and prints
    estimated BQ write throughput at an assumed 100 MB/s publish rate.
    No pipeline is launched and nothing is published.

    Args:
        num_messages: Total number of messages that would be published.
        message_size_bytes: Target size per message in bytes.
    """
    sample_count = 10_000
    logging.info("Generating %d sample messages...", sample_count)

    sizes: list[int] = []
    for _ in range(sample_count):
        msg = generate_message(message_size_bytes)
        sizes.append(len(msg.encode("utf-8")))

    avg_size = sum(sizes) / len(sizes)
    min_size = min(sizes)
    max_size = max(sizes)

    # Assume 100 MB/s publish rate for BQ throughput estimates.
    assumed_mbps = 100.0
    rates = calculate_rates(assumed_mbps, int(avg_size))

    total_bytes = num_messages * message_size_bytes
    total_gb = total_bytes / 1_000_000_000

    print("\n=== Dry Run Report ===")
    print(f"Sample messages generated: {sample_count:,}")
    print(f"Target message size:       {message_size_bytes:,} bytes")
    print(
        f"Actual message size:       avg={avg_size:.0f}, "
        f"min={min_size}, max={max_size} bytes"
    )
    print()
    print(f"Total messages:            {num_messages:,}")
    print(f"Total data:                ~{total_gb:.0f} GB")
    print()
    print(f"At assumed {assumed_mbps:.0f} MB/s publish rate:")
    print(f"  Publish rate:            {rates['msgs_per_sec']:,.0f} msgs/sec")
    print(f"  Pub/Sub throughput:      {rates['publish_mbps']:.1f} MB/s")
    print(
        f"  Pub/Sub streams needed:  {rates['pubsub_streams_needed']:.0f} "
        f"(at 10 MB/s per StreamingPull)"
    )
    print()
    print(
        f"  BQ row size (estimated): {rates['bq_row_size_bytes']:.0f} bytes "
        f"(payload + {_BQ_METADATA_OVERHEAD_BYTES} bytes metadata)"
    )
    print(f"  BQ write throughput:     {rates['bq_write_mbps']:.1f} MB/s")
    print()
    print(f"  BQ Storage Write API quota usage (per job, {assumed_mbps:.0f} MB/s):")
    print(f"    Regional (300 MB/s):   {rates['bq_write_mbps'] / 300 * 100:.1f}%")
    print(f"    Multi-region (3 GB/s): {rates['bq_write_mbps'] / 3000 * 100:.1f}%")
    print()
    print("  Two jobs at this rate:")
    two_job_mbps = rates["bq_write_mbps"] * 2
    print(f"    Combined BQ write:     {two_job_mbps:.1f} MB/s")
    print(f"    Regional quota usage:  {two_job_mbps / 300 * 100:.1f}%")
    print()

    # Verify a sample message is valid JSON with expected fields
    sample = generate_message(message_size_bytes)
    parsed = json.loads(sample)
    expected_fields = [
        "ride_id",
        "point_idx",
        "latitude",
        "longitude",
        "timestamp",
        "meter_reading",
        "meter_increment",
        "ride_status",
        "passenger_count",
    ]
    missing = [f for f in expected_fields if f not in parsed]
    if missing:
        print(f"WARNING: Sample message missing fields: {missing}")
    else:
        print("Sample message validation: OK (valid JSON, all taxi fields present)")
    print()


def run(argv: list[str] | None = None):
    """Runs the synthetic data publisher pipeline.

    In validate-only mode, validates message sizes and prints rate estimates
    without launching a pipeline. Otherwise, generates messages and
    publishes them to the configured Pub/Sub topic via Dataflow.

    Args:
        argv: Command-line arguments.
    """
    pipeline_options = PipelineOptions(argv)
    custom_options = pipeline_options.view_as(PublisherPipelineOptions)

    num_messages = custom_options.num_messages
    message_size_bytes = custom_options.message_size_bytes

    if custom_options.validate_only:
        run_validate_only(num_messages, message_size_bytes)
        return

    topic = custom_options.topic
    if not topic:
        raise ValueError("--topic is required for the publisher pipeline.")

    # Calculate messages per seed for even distribution
    msgs_per_seed = num_messages // _NUM_SEEDS
    remainder = num_messages % _NUM_SEEDS

    total_bytes = num_messages * message_size_bytes
    total_gb = total_bytes / 1_000_000_000

    logging.info("Publisher pipeline configuration:")
    logging.info("  Topic:              %s", topic)
    logging.info("  Total messages:     %d", num_messages)
    logging.info("  Message size:       %d bytes", message_size_bytes)
    logging.info("  Total data:         %.1f GB", total_gb)
    logging.info("  Seeds:              %d", _NUM_SEEDS)
    logging.info("  Messages per seed:  %d (+%d remainder)", msgs_per_seed, remainder)

    # Verify message size with a sample
    sample = generate_message(message_size_bytes)
    sample_size = len(sample.encode("utf-8"))
    logging.info(
        "  Sample message size: %d bytes (target: %d)", sample_size, message_size_bytes
    )

    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Create seed elements for parallelism. Each seed produces a
        # batch of messages. The first `remainder` seeds get one extra
        # message to account for integer division.
        seeds_with_counts = []
        for i in range(_NUM_SEEDS):
            count = msgs_per_seed + (1 if i < remainder else 0)
            if count > 0:
                seeds_with_counts.append((i, count))

        (
            pipeline
            | "CreateSeeds" >> beam.Create(seeds_with_counts)
            | "GenerateMessages"
            >> beam.FlatMap(
                lambda seed_count, size=message_size_bytes: generate_batch(
                    seed_count[0], seed_count[1], size
                )
            )
            | "PublishToPubSub" >> pubsub.WriteToPubSub(topic=topic)
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()

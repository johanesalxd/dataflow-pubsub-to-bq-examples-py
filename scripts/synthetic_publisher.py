"""High-throughput synthetic data publisher for BQ throughput testing.

Generates realistic taxi ride JSON messages at a configurable MB/s rate
and publishes them to a Pub/Sub topic. Message generation is broker-agnostic
so it can be reused with Kafka or other message brokers.

The publisher supports a --dry-run mode that calculates and reports message
sizes, publish rates, and estimated BQ write throughput without actually
publishing any messages.

Usage:
    # Dry run -- validate sizes and math
    python scripts/synthetic_publisher.py \
        --project=my-project \
        --topic=projects/my-project/topics/perf_test_a \
        --target_mbps=100 \
        --message_size_bytes=10000 \
        --dry-run

    # Live publish for 30 minutes
    python scripts/synthetic_publisher.py \
        --project=my-project \
        --topic=projects/my-project/topics/perf_test_a \
        --target_mbps=100 \
        --message_size_bytes=10000 \
        --duration_minutes=30
"""

import argparse
import json
import logging
import random
import signal
import string
import time
from typing import Any

from google.cloud import pubsub_v1

logger = logging.getLogger(__name__)

# --- Constants ---

_RIDE_STATUSES = ["enroute", "pickup", "dropoff", "waiting"]

# Approximate overhead added by the pipeline when constructing a BQ row.
# Includes: subscription_name, message_id, publish_time, processing_time,
# attributes -- serialized as protobuf fields in the Storage Write API.
_BQ_METADATA_OVERHEAD_BYTES = 128

# Pub/Sub batch publisher settings for high throughput.
_BATCH_MAX_MESSAGES = 1000
_BATCH_MAX_BYTES = 10_000_000  # 10 MB (Pub/Sub request size limit)
_BATCH_MAX_LATENCY = 0.01  # 10 ms


# =========================================================================
# Broker-Agnostic Message Generation
# =========================================================================


def generate_message(message_size_bytes: int) -> str:
    """Generates a single synthetic taxi ride JSON message.

    Produces a valid JSON object containing the 9 standard taxi ride fields
    with randomized values, plus a ``_padding`` field filled with random
    characters to reach the target byte size. The output is compatible with
    both ``pipeline.py`` (field-by-field parsing) and ``pipeline_json.py``
    (raw JSON column).

    Args:
        message_size_bytes: Target total size of the JSON string in bytes.
            Must be at least 250 bytes to fit the base taxi fields.

    Returns:
        A JSON string of approximately ``message_size_bytes`` bytes.

    Raises:
        ValueError: If message_size_bytes is less than 250.
    """
    if message_size_bytes < 250:
        raise ValueError(f"message_size_bytes must be >= 250, got {message_size_bytes}")

    base_payload: dict[str, Any] = {
        "ride_id": f"ride-{random.randint(100000, 999999)}",
        "point_idx": random.randint(0, 2000),
        "latitude": round(random.uniform(1.2, 1.5), 6),
        "longitude": round(random.uniform(103.6, 104.0), 6),
        "timestamp": f"2026-02-{random.randint(1, 28):02d}"
        f"T{random.randint(0, 23):02d}:{random.randint(0, 59):02d}:"
        f"{random.randint(0, 59):02d}.{random.randint(0, 999999):06d}+08:00",
        "meter_reading": round(random.uniform(0.0, 100.0), 2),
        "meter_increment": round(random.uniform(0.01, 0.10), 4),
        "ride_status": random.choice(_RIDE_STATUSES),
        "passenger_count": random.randint(1, 6),
    }

    # Measure base size without padding
    base_payload["_padding"] = ""
    base_json = json.dumps(base_payload, separators=(",", ":"))
    base_size = len(base_json.encode("utf-8"))

    # Calculate padding needed
    padding_needed = message_size_bytes - base_size
    if padding_needed > 0:
        base_payload["_padding"] = "".join(
            random.choices(string.ascii_lowercase, k=padding_needed)
        )

    result = json.dumps(base_payload, separators=(",", ":"))
    return result


def calculate_rates(target_mbps: float, message_size_bytes: int) -> dict[str, float]:
    """Calculates publish rates and estimated BQ throughput.

    Pure computation with no broker dependency. Can be used by any
    publisher implementation (Pub/Sub, Kafka, etc.).

    Args:
        target_mbps: Target publish throughput in megabytes per second.
        message_size_bytes: Size of each message in bytes.

    Returns:
        Dictionary with:
        - msgs_per_sec: Required messages per second.
        - publish_mbps: Actual publish throughput in MB/s.
        - bq_row_size_bytes: Estimated BQ row size (payload + metadata).
        - bq_write_mbps: Estimated BQ write throughput in MB/s.
        - pubsub_streams_needed: Minimum StreamingPull streams required.
    """
    target_bytes_per_sec = target_mbps * 1_000_000
    msgs_per_sec = target_bytes_per_sec / message_size_bytes
    publish_mbps = (msgs_per_sec * message_size_bytes) / 1_000_000

    bq_row_size = message_size_bytes + _BQ_METADATA_OVERHEAD_BYTES
    bq_write_mbps = (msgs_per_sec * bq_row_size) / 1_000_000

    # Pub/Sub StreamingPull: 10 MB/s per stream
    pubsub_streams_needed = target_mbps / 10.0

    return {
        "msgs_per_sec": msgs_per_sec,
        "publish_mbps": publish_mbps,
        "bq_row_size_bytes": float(bq_row_size),
        "bq_write_mbps": bq_write_mbps,
        "pubsub_streams_needed": pubsub_streams_needed,
    }


# =========================================================================
# Dry Run
# =========================================================================


def run_dry_run(target_mbps: float, message_size_bytes: int) -> None:
    """Generates sample messages and reports size/rate calculations.

    Args:
        target_mbps: Target publish throughput in MB/s.
        message_size_bytes: Target size per message in bytes.
    """
    sample_count = 10_000
    logger.info("Generating %d sample messages...", sample_count)

    sizes: list[int] = []
    for _ in range(sample_count):
        msg = generate_message(message_size_bytes)
        sizes.append(len(msg.encode("utf-8")))

    avg_size = sum(sizes) / len(sizes)
    min_size = min(sizes)
    max_size = max(sizes)

    rates = calculate_rates(target_mbps, int(avg_size))

    print("\n=== Dry Run Report ===")
    print(f"Sample messages generated: {sample_count:,}")
    print(f"Target message size:       {message_size_bytes:,} bytes")
    print(
        f"Actual message size:       avg={avg_size:.0f}, "
        f"min={min_size}, max={max_size} bytes"
    )
    print()
    print(f"At --target_mbps={target_mbps}:")
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
    print("  BQ Storage Write API quota usage (per job):")
    print(f"    Regional (300 MB/s):   {rates['bq_write_mbps'] / 300 * 100:.1f}%")
    print(f"    Multi-region (3 GB/s): {rates['bq_write_mbps'] / 3000 * 100:.1f}%")
    print()
    print("  Two jobs at this rate:")
    two_job_mbps = rates["bq_write_mbps"] * 2
    print(f"    Combined BQ write:     {two_job_mbps:.1f} MB/s")
    print(f"    Regional quota usage:  {two_job_mbps / 300 * 100:.1f}%")
    print()

    # Verify a sample message is valid JSON and parseable
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


# =========================================================================
# Pub/Sub Publisher
# =========================================================================


def run_publisher(
    project: str,
    topic: str,
    target_mbps: float,
    message_size_bytes: int,
    duration_minutes: float,
) -> None:
    """Publishes synthetic messages to a Pub/Sub topic at a target rate.

    Uses a token bucket rate limiter to sustain the target MB/s throughput.
    Logs progress every 5 seconds and shuts down gracefully on SIGINT/SIGTERM.

    Args:
        project: GCP project ID.
        topic: Full Pub/Sub topic path (projects/P/topics/T).
        target_mbps: Target publish throughput in MB/s.
        message_size_bytes: Target size per message in bytes.
        duration_minutes: How long to run (0 = infinite).
    """
    rates = calculate_rates(target_mbps, message_size_bytes)
    target_bytes_per_sec = target_mbps * 1_000_000

    logger.info("Starting publisher")
    logger.info("  Topic:            %s", topic)
    logger.info("  Target:           %.1f MB/s", target_mbps)
    logger.info("  Message size:     %d bytes", message_size_bytes)
    logger.info("  Publish rate:     %.0f msgs/sec", rates["msgs_per_sec"])
    logger.info("  Est. BQ write:    %.1f MB/s", rates["bq_write_mbps"])
    if duration_minutes > 0:
        logger.info("  Duration:         %.0f minutes", duration_minutes)
    else:
        logger.info("  Duration:         infinite (Ctrl+C to stop)")

    # Configure batch settings for high throughput
    batch_settings = pubsub_v1.types.BatchSettings(
        max_messages=_BATCH_MAX_MESSAGES,
        max_bytes=_BATCH_MAX_BYTES,
        max_latency=_BATCH_MAX_LATENCY,
    )
    publisher = pubsub_v1.PublisherClient(batch_settings=batch_settings)

    # Tracking state
    total_bytes = 0
    total_messages = 0
    error_count = 0
    running = True
    start_time = time.monotonic()
    last_log_time = start_time
    last_log_bytes = 0
    last_log_messages = 0

    def publish_callback(future, msg_bytes: int) -> None:
        """Handles publish result."""
        nonlocal error_count
        try:
            future.result(timeout=30)
        except Exception as e:
            error_count += 1
            if error_count <= 10:
                logger.error("Publish failed: %s", e)

    def shutdown(signum, frame):
        """Handles graceful shutdown."""
        nonlocal running
        logger.info("Shutdown signal received, finishing...")
        running = False

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    end_time = (
        start_time + duration_minutes * 60 if duration_minutes > 0 else float("inf")
    )

    logger.info("Publishing started")

    try:
        while running and time.monotonic() < end_time:
            now = time.monotonic()
            elapsed = now - start_time

            # Token bucket: how many bytes should we have sent by now?
            expected_bytes = elapsed * target_bytes_per_sec

            # If we're ahead of schedule, sleep briefly
            if total_bytes >= expected_bytes:
                time.sleep(0.001)
                continue

            # Generate and publish a message
            msg = generate_message(message_size_bytes)
            msg_bytes = len(msg.encode("utf-8"))

            future = publisher.publish(topic, data=msg.encode("utf-8"))
            future.add_done_callback(lambda f, mb=msg_bytes: publish_callback(f, mb))

            total_bytes += msg_bytes
            total_messages += 1

            # Progress log every 5 seconds
            if now - last_log_time >= 5.0:
                interval = now - last_log_time
                interval_bytes = total_bytes - last_log_bytes
                interval_msgs = total_messages - last_log_messages
                current_mbps = interval_bytes / interval / 1_000_000
                current_mps = interval_msgs / interval

                logger.info(
                    "Progress: %.1f MB/s (target: %.1f) | "
                    "%.0f msgs/sec | total: %.1f MB / %d msgs | "
                    "errors: %d",
                    current_mbps,
                    target_mbps,
                    current_mps,
                    total_bytes / 1_000_000,
                    total_messages,
                    error_count,
                )

                last_log_time = now
                last_log_bytes = total_bytes
                last_log_messages = total_messages

    finally:
        elapsed = time.monotonic() - start_time
        avg_mbps = total_bytes / elapsed / 1_000_000 if elapsed > 0 else 0

        logger.info("Shutting down publisher, flushing pending messages...")
        publisher.transport.close()

        logger.info(
            "Done. Total: %.1f MB, %d messages in %.1f seconds "
            "(avg: %.1f MB/s, errors: %d)",
            total_bytes / 1_000_000,
            total_messages,
            elapsed,
            avg_mbps,
            error_count,
        )


# =========================================================================
# CLI Entry Point
# =========================================================================


def main():
    """Entry point for the synthetic publisher."""
    parser = argparse.ArgumentParser(
        description="High-throughput synthetic data publisher for BQ "
        "throughput testing."
    )
    parser.add_argument(
        "--project",
        required=True,
        help="GCP project ID.",
    )
    parser.add_argument(
        "--topic",
        required=True,
        help="Full Pub/Sub topic path (e.g., projects/my-project/topics/perf_test_a).",
    )
    parser.add_argument(
        "--target_mbps",
        type=float,
        default=100.0,
        help="Target publish throughput in MB/s (default: 100).",
    )
    parser.add_argument(
        "--message_size_bytes",
        type=int,
        default=10000,
        help="Target size per message in bytes (default: 10000).",
    )
    parser.add_argument(
        "--duration_minutes",
        type=float,
        default=30.0,
        help="How long to run in minutes (0 = infinite, default: 30).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Calculate and report sizes/rates without publishing.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if args.dry_run:
        run_dry_run(args.target_mbps, args.message_size_bytes)
    else:
        run_publisher(
            project=args.project,
            topic=args.topic,
            target_mbps=args.target_mbps,
            message_size_bytes=args.message_size_bytes,
            duration_minutes=args.duration_minutes,
        )


if __name__ == "__main__":
    main()

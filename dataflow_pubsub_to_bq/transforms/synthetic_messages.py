"""Broker-agnostic synthetic message generation for throughput testing.

Generates realistic taxi ride messages in JSON or Avro format at
configurable sizes for use by both local publishers and Dataflow
publisher pipelines. All functions are pure computation with no GCP
or broker dependencies.
"""

import io
import json
import random
import string
from typing import Any

import fastavro

# --- Constants ---

_RIDE_STATUSES = ["enroute", "pickup", "dropoff", "waiting"]

# Approximate overhead added by the pipeline when constructing a BQ row.
# Includes: subscription_name, message_id, publish_time, processing_time,
# attributes -- serialized as protobuf fields in the Storage Write API.
_BQ_METADATA_OVERHEAD_BYTES = 128

# Avro schema for the taxi ride record (v1). Embedded as a constant so it
# works on Dataflow workers without needing to package .avsc files.
# Matches schemas/taxi_ride_v1.avsc exactly.
_TAXI_RIDE_V1_AVRO_SCHEMA = {
    "type": "record",
    "name": "TaxiRide",
    "namespace": "com.example.taxi",
    "fields": [
        {"name": "ride_id", "type": "string"},
        {"name": "point_idx", "type": "int"},
        {"name": "latitude", "type": "double"},
        {"name": "longitude", "type": "double"},
        {
            "name": "timestamp",
            "type": {"type": "string", "logicalType": "iso-datetime"},
        },
        {"name": "meter_reading", "type": "double"},
        {"name": "meter_increment", "type": "double"},
        {"name": "ride_status", "type": "string"},
        {"name": "passenger_count", "type": "int"},
    ],
}

_PARSED_AVRO_SCHEMA = fastavro.parse_schema(_TAXI_RIDE_V1_AVRO_SCHEMA)


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


def generate_message_pool(
    message_size_bytes: int, pool_size: int = 1000
) -> list[bytes]:
    """Pre-generates a pool of encoded messages for high-throughput publishing.

    Generates ``pool_size`` messages at startup so the hot publish loop can
    cycle through pre-built ``bytes`` objects instead of calling
    ``generate_message()`` + ``json.dumps()`` + ``.encode()`` on every
    iteration.  This eliminates the CPU bottleneck of random string
    generation and JSON serialization from the critical path.

    Args:
        message_size_bytes: Target size per message in bytes.
        pool_size: Number of messages to pre-generate (default 1000).

    Returns:
        A list of UTF-8 encoded message bytes, each approximately
        ``message_size_bytes`` in length.
    """
    pool: list[bytes] = []
    for _ in range(pool_size):
        pool.append(generate_message(message_size_bytes).encode("utf-8"))
    return pool


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


# --- Avro message generation ---


def generate_avro_record() -> dict[str, Any]:
    """Generates a single synthetic taxi ride record as a plain dict.

    Produces the same 9 taxi ride fields as ``generate_message()`` but
    without the ``_padding`` field and without JSON serialization. The
    output dict is suitable for Avro binary serialization using the
    ``taxi_ride_v1`` schema.

    Returns:
        A dictionary with the 9 standard taxi ride fields.
    """
    return {
        "ride_id": f"ride-{random.randint(100000, 999999)}",
        "point_idx": random.randint(0, 2000),
        "latitude": round(random.uniform(1.2, 1.5), 6),
        "longitude": round(random.uniform(103.6, 104.0), 6),
        "timestamp": (
            f"2026-02-{random.randint(1, 28):02d}"
            f"T{random.randint(0, 23):02d}:{random.randint(0, 59):02d}:"
            f"{random.randint(0, 59):02d}.{random.randint(0, 999999):06d}+08:00"
        ),
        "meter_reading": round(random.uniform(0.0, 100.0), 2),
        "meter_increment": round(random.uniform(0.01, 0.10), 4),
        "ride_status": random.choice(_RIDE_STATUSES),
        "passenger_count": random.randint(1, 6),
    }


def serialize_avro_record(record: dict[str, Any]) -> bytes:
    """Serializes a taxi ride dict to Avro binary using schemaless encoding.

    Uses ``fastavro.schemaless_writer`` which writes only the data bytes
    (no container header, no embedded schema). The consumer must know
    the schema a priori to deserialize — matching the Kafka + Schema
    Registry pattern where schema is cached on the consumer side.

    Args:
        record: A dictionary matching the taxi_ride_v1 Avro schema.

    Returns:
        Avro binary encoded bytes (typically ~85 bytes per record).
    """
    buffer = io.BytesIO()
    fastavro.schemaless_writer(buffer, _PARSED_AVRO_SCHEMA, record)
    return buffer.getvalue()


def generate_avro_message_pool(pool_size: int = 1000) -> list[bytes]:
    """Pre-generates a pool of Avro-encoded messages for high-throughput publishing.

    Same pooling strategy as ``generate_message_pool()`` but produces
    Avro binary bytes instead of JSON. Message size is determined by
    the schema (~85 bytes per record) with no padding.

    Args:
        pool_size: Number of messages to pre-generate (default 1000).

    Returns:
        A list of Avro binary encoded message bytes.
    """
    pool: list[bytes] = []
    for _ in range(pool_size):
        record = generate_avro_record()
        pool.append(serialize_avro_record(record))
    return pool


def calculate_avro_rates(
    target_mbps: float, avro_message_size: float
) -> dict[str, float]:
    """Calculates publish rates and estimated BQ throughput for Avro format.

    Like ``calculate_rates()`` but accounts for the Avro-to-TableRow
    expansion ratio. Avro binary is compact (~85 bytes) while the
    Dataflow-serialized TableRow is larger (~400 bytes), producing
    a ~4-5x expansion visible on the Dataflow metrics dashboard.

    Args:
        target_mbps: Target Avro publish throughput in megabytes per second.
        avro_message_size: Average size of each Avro message in bytes.

    Returns:
        Dictionary with:
        - msgs_per_sec: Required messages per second.
        - avro_publish_mbps: Avro wire throughput in MB/s.
        - estimated_tablerow_size: Estimated Dataflow TableRow size in bytes.
        - bq_sink_mbps: Estimated BQ sink throughput on Dataflow dashboard.
        - expansion_ratio: TableRow size / Avro size.
        - pubsub_streams_needed: Minimum StreamingPull streams required.
    """
    target_bytes_per_sec = target_mbps * 1_000_000
    msgs_per_sec = target_bytes_per_sec / avro_message_size
    avro_publish_mbps = (msgs_per_sec * avro_message_size) / 1_000_000

    # Estimated Dataflow TableRow size: 14 fields (9 data + 5 envelope)
    # with field names, type metadata, and string-encoded values.
    estimated_tablerow_size = avro_message_size + _BQ_METADATA_OVERHEAD_BYTES + 200
    expansion_ratio = estimated_tablerow_size / avro_message_size
    bq_sink_mbps = (msgs_per_sec * estimated_tablerow_size) / 1_000_000

    # Pub/Sub StreamingPull: 10 MB/s per stream
    pubsub_streams_needed = target_mbps / 10.0

    return {
        "msgs_per_sec": msgs_per_sec,
        "avro_publish_mbps": avro_publish_mbps,
        "estimated_tablerow_size": estimated_tablerow_size,
        "bq_sink_mbps": bq_sink_mbps,
        "expansion_ratio": expansion_ratio,
        "pubsub_streams_needed": pubsub_streams_needed,
    }

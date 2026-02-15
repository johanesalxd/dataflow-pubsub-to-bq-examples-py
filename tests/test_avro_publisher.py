"""Unit tests for Avro message generation and publisher pipeline integration.

Tests the Avro serialization functions in synthetic_messages.py and
the generate_avro_batch function used by the publisher pipeline.
All tests are pure computation -- no Pub/Sub writes.
"""

import io

import apache_beam as beam
import fastavro
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from dataflow_pubsub_to_bq.pipeline_publisher import generate_avro_batch
from dataflow_pubsub_to_bq.transforms.synthetic_messages import (
    _PARSED_AVRO_SCHEMA,
    _RIDE_STATUSES,
    calculate_avro_rates,
    generate_avro_message_pool,
    generate_avro_record,
    serialize_avro_record,
)

_EXPECTED_FIELDS = [
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


def test_generate_avro_record_fields():
    """Verifies Avro record contains all 9 taxi fields and no padding."""
    record = generate_avro_record()
    for field in _EXPECTED_FIELDS:
        assert field in record, f"Missing field: {field}"
    assert "_padding" not in record


def test_generate_avro_record_types():
    """Verifies Avro record field value types match the schema."""
    record = generate_avro_record()
    assert isinstance(record["ride_id"], str)
    assert isinstance(record["point_idx"], int)
    assert isinstance(record["latitude"], float)
    assert isinstance(record["longitude"], float)
    assert isinstance(record["timestamp"], str)
    assert isinstance(record["meter_reading"], float)
    assert isinstance(record["meter_increment"], float)
    assert isinstance(record["ride_status"], str)
    assert isinstance(record["passenger_count"], int)


def test_generate_avro_record_ride_status_values():
    """Verifies ride_status is one of the 4 expected values."""
    for _ in range(100):
        record = generate_avro_record()
        assert record["ride_status"] in _RIDE_STATUSES, (
            f"Unexpected ride_status: {record['ride_status']}"
        )


def test_serialize_avro_record_returns_bytes():
    """Verifies Avro serialization produces bytes."""
    record = generate_avro_record()
    avro_bytes = serialize_avro_record(record)
    assert isinstance(avro_bytes, bytes)


def test_avro_message_size():
    """Verifies Avro messages are in the expected ~80-100 byte range."""
    sizes = []
    for _ in range(1000):
        record = generate_avro_record()
        avro_bytes = serialize_avro_record(record)
        sizes.append(len(avro_bytes))

    avg_size = sum(sizes) / len(sizes)
    assert 60 <= avg_size <= 120, (
        f"Average Avro size {avg_size:.0f} outside expected range"
    )


def test_avro_round_trip():
    """Verifies Avro serialize/deserialize round-trip preserves all fields."""
    record = generate_avro_record()
    avro_bytes = serialize_avro_record(record)

    # Deserialize using fastavro
    buffer = io.BytesIO(avro_bytes)
    deserialized = fastavro.schemaless_reader(buffer, _PARSED_AVRO_SCHEMA)

    for field in _EXPECTED_FIELDS:
        assert deserialized[field] == record[field], (
            f"Field {field}: expected {record[field]}, got {deserialized[field]}"
        )


def test_generate_avro_message_pool_returns_bytes():
    """Verifies Avro message pool entries are bytes."""
    pool = generate_avro_message_pool(pool_size=10)
    for msg in pool:
        assert isinstance(msg, bytes)


def test_generate_avro_message_pool_size():
    """Verifies Avro message pool has the requested size."""
    pool = generate_avro_message_pool(pool_size=50)
    assert len(pool) == 50


def test_generate_avro_batch_correct_count():
    """Verifies generate_avro_batch produces the requested number of messages."""
    messages = generate_avro_batch((0, 50))
    assert len(messages) == 50


def test_generate_avro_batch_zero_messages():
    """Verifies generate_avro_batch handles zero messages gracefully."""
    messages = generate_avro_batch((0, 0))
    assert len(messages) == 0


def test_generate_avro_batch_valid_avro():
    """Verifies every message from generate_avro_batch is valid Avro."""
    messages = generate_avro_batch((0, 20))
    for msg in messages:
        buffer = io.BytesIO(msg)
        record = fastavro.schemaless_reader(buffer, _PARSED_AVRO_SCHEMA)
        assert "ride_id" in record
        assert "ride_status" in record
        assert record["ride_status"] in _RIDE_STATUSES


def test_pipeline_avro_produces_correct_count():
    """Verifies the pipeline stage produces the correct total Avro message count."""
    num_seeds = 3
    msgs_per_seed = 10
    seeds_with_counts = [(i, msgs_per_seed) for i in range(num_seeds)]

    with TestPipeline() as p:
        messages = (
            p | beam.Create(seeds_with_counts) | beam.FlatMap(generate_avro_batch)
        )
        count = messages | beam.combiners.Count.Globally()
        assert_that(
            count,
            equal_to([num_seeds * msgs_per_seed]),
            label="CheckAvroCount",
        )


def test_calculate_avro_rates():
    """Verifies Avro rate calculations produce expected expansion ratio."""
    rates = calculate_avro_rates(target_mbps=25.0, avro_message_size=85.0)

    assert abs(rates["avro_publish_mbps"] - 25.0) < 0.01
    assert rates["msgs_per_sec"] > 200_000
    assert rates["expansion_ratio"] > 3.0
    assert rates["bq_sink_mbps"] > rates["avro_publish_mbps"]

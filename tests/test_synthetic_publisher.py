"""Unit tests for synthetic publisher message generation and rate math.

All tests are pure computation -- no GCP resources, no mocks, no network.
"""

import json
import sys
from pathlib import Path

import pytest

# Add scripts/ to import path so we can import synthetic_publisher directly.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from synthetic_publisher import (  # noqa: E402
    _BQ_METADATA_OVERHEAD_BYTES,
    calculate_rates,
    generate_message,
)


# =========================================================================
# generate_message tests
# =========================================================================


def test_generate_message_is_valid_json():
    """Verifies generated message is valid JSON."""
    msg = generate_message(1024)
    parsed = json.loads(msg)
    assert isinstance(parsed, dict)


def test_generate_message_contains_taxi_fields():
    """Verifies all 9 taxi ride fields are present in generated message."""
    msg = generate_message(1024)
    parsed = json.loads(msg)

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
    for field in expected_fields:
        assert field in parsed, f"Missing field: {field}"


def test_generate_message_contains_padding():
    """Verifies padding field exists for large message sizes."""
    msg = generate_message(1024)
    parsed = json.loads(msg)
    assert "_padding" in parsed
    assert len(parsed["_padding"]) > 0


def test_generate_message_size_1kb():
    """Verifies message size is within 5% of 1024 byte target."""
    sizes = [len(generate_message(1024).encode("utf-8")) for _ in range(100)]
    avg = sum(sizes) / len(sizes)
    assert abs(avg - 1024) / 1024 < 0.05, f"Average size {avg} not within 5% of 1024"


def test_generate_message_size_4kb():
    """Verifies message size is within 5% of 4096 byte target."""
    sizes = [len(generate_message(4096).encode("utf-8")) for _ in range(100)]
    avg = sum(sizes) / len(sizes)
    assert abs(avg - 4096) / 4096 < 0.05, f"Average size {avg} not within 5% of 4096"


def test_generate_message_size_10kb():
    """Verifies message size is within 5% of 10000 byte target."""
    sizes = [len(generate_message(10000).encode("utf-8")) for _ in range(100)]
    avg = sum(sizes) / len(sizes)
    assert abs(avg - 10000) / 10000 < 0.05, f"Average size {avg} not within 5% of 10000"


def test_generate_message_minimum_size():
    """Verifies behavior at minimum allowed size (250 bytes)."""
    msg = generate_message(250)
    parsed = json.loads(msg)
    assert "ride_id" in parsed


def test_generate_message_below_minimum_raises():
    """Verifies ValueError for message_size_bytes below 250."""
    with pytest.raises(ValueError, match="must be >= 250"):
        generate_message(100)


def test_generate_message_field_types():
    """Verifies generated fields have correct types."""
    msg = generate_message(1024)
    parsed = json.loads(msg)

    assert isinstance(parsed["ride_id"], str)
    assert isinstance(parsed["point_idx"], int)
    assert isinstance(parsed["latitude"], float)
    assert isinstance(parsed["longitude"], float)
    assert isinstance(parsed["timestamp"], str)
    assert isinstance(parsed["meter_reading"], (int, float))
    assert isinstance(parsed["meter_increment"], (int, float))
    assert isinstance(parsed["ride_status"], str)
    assert isinstance(parsed["passenger_count"], int)


def test_generate_message_randomness():
    """Verifies messages are not identical (randomized)."""
    msgs = {generate_message(1024) for _ in range(10)}
    assert len(msgs) > 1, "All 10 messages were identical"


# =========================================================================
# calculate_rates tests
# =========================================================================


def test_calculate_rates_100mbps_10kb():
    """Verifies rate math: 100 MB/s at 10000 bytes = 10000 msgs/sec."""
    rates = calculate_rates(target_mbps=100.0, message_size_bytes=10000)

    assert rates["msgs_per_sec"] == pytest.approx(10000.0, rel=0.01)
    assert rates["publish_mbps"] == pytest.approx(100.0, rel=0.01)


def test_calculate_rates_100mbps_4kb():
    """Verifies rate math: 100 MB/s at 4096 bytes = ~24414 msgs/sec."""
    rates = calculate_rates(target_mbps=100.0, message_size_bytes=4096)

    expected_mps = 100_000_000 / 4096
    assert rates["msgs_per_sec"] == pytest.approx(expected_mps, rel=0.01)


def test_calculate_rates_bq_overhead():
    """Verifies BQ row size includes metadata overhead."""
    rates = calculate_rates(target_mbps=100.0, message_size_bytes=10000)

    expected_bq_row = 10000 + _BQ_METADATA_OVERHEAD_BYTES
    assert rates["bq_row_size_bytes"] == pytest.approx(expected_bq_row, rel=0.01)


def test_calculate_rates_bq_write_mbps():
    """Verifies BQ write throughput accounts for metadata inflation."""
    rates = calculate_rates(target_mbps=100.0, message_size_bytes=10000)

    # BQ write = msgs_per_sec * (message_size + overhead) / 1M
    expected_bq_mbps = 10000 * (10000 + _BQ_METADATA_OVERHEAD_BYTES) / 1_000_000
    assert rates["bq_write_mbps"] == pytest.approx(expected_bq_mbps, rel=0.01)


def test_calculate_rates_pubsub_streams():
    """Verifies StreamingPull stream count calculation."""
    rates = calculate_rates(target_mbps=100.0, message_size_bytes=10000)

    # 100 MB/s / 10 MB/s per stream = 10 streams
    assert rates["pubsub_streams_needed"] == pytest.approx(10.0, rel=0.01)


def test_calculate_rates_24mbps():
    """Verifies rate math at 24 MB/s (Kafka-equivalent scenario)."""
    rates = calculate_rates(target_mbps=24.0, message_size_bytes=10000)

    assert rates["msgs_per_sec"] == pytest.approx(2400.0, rel=0.01)
    assert rates["publish_mbps"] == pytest.approx(24.0, rel=0.01)

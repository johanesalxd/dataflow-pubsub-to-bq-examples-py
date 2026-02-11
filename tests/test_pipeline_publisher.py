"""Unit tests for the publisher pipeline message generation stage.

Tests the generate_batch function and pipeline message output using
TestPipeline. All tests are pure computation -- no Pub/Sub writes.
"""

import json

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from dataflow_pubsub_to_bq.pipeline_publisher import generate_batch


def test_generate_batch_correct_count():
    """Verifies generate_batch produces the requested number of messages."""
    messages = generate_batch(seed=0, num_messages=50, message_size_bytes=1024)
    assert len(messages) == 50


def test_generate_batch_returns_bytes():
    """Verifies generate_batch returns bytes, not str."""
    messages = generate_batch(seed=0, num_messages=5, message_size_bytes=1024)
    for msg in messages:
        assert isinstance(msg, bytes)


def test_generate_batch_valid_json():
    """Verifies every message from generate_batch is valid JSON."""
    messages = generate_batch(seed=0, num_messages=20, message_size_bytes=1024)
    for msg in messages:
        parsed = json.loads(msg.decode("utf-8"))
        assert isinstance(parsed, dict)
        assert "ride_id" in parsed
        assert "passenger_count" in parsed


def test_generate_batch_message_sizes():
    """Verifies messages are within 5% of the target size."""
    target = 10000
    messages = generate_batch(seed=0, num_messages=100, message_size_bytes=target)
    avg_size = sum(len(m) for m in messages) / len(messages)
    assert abs(avg_size - target) / target < 0.05, (
        f"Average size {avg_size} not within 5% of {target}"
    )


def test_generate_batch_zero_messages():
    """Verifies generate_batch handles zero messages gracefully."""
    messages = generate_batch(seed=0, num_messages=0, message_size_bytes=1024)
    assert len(messages) == 0


def test_pipeline_produces_correct_count():
    """Verifies the pipeline stage produces the correct total message count."""
    num_seeds = 3
    msgs_per_seed = 10
    seeds_with_counts = [(i, msgs_per_seed) for i in range(num_seeds)]

    with TestPipeline() as p:
        messages = (
            p
            | beam.Create(seeds_with_counts)
            | beam.FlatMap(lambda sc, size=500: generate_batch(sc[0], sc[1], size))
        )
        # Count elements by mapping to 1 and combining
        count = messages | beam.combiners.Count.Globally()
        assert_that(
            count,
            equal_to([num_seeds * msgs_per_seed]),
            label="CheckCount",
        )


def test_pipeline_messages_are_bytes():
    """Verifies pipeline output elements are bytes."""
    with TestPipeline() as p:
        messages = (
            p
            | beam.Create([(0, 5)])
            | beam.FlatMap(lambda sc, size=500: generate_batch(sc[0], sc[1], size))
        )
        # Verify all elements are bytes by checking type
        type_checks = messages | beam.Map(lambda m: isinstance(m, bytes))
        assert_that(
            type_checks,
            equal_to([True] * 5),
            label="CheckBytes",
        )


def test_pipeline_uneven_distribution():
    """Verifies remainder messages are distributed correctly."""
    # 25 messages across 3 seeds: 8+8+9 or similar
    total = 25
    num_seeds = 3
    msgs_per_seed = total // num_seeds
    remainder = total % num_seeds

    seeds_with_counts = []
    for i in range(num_seeds):
        count = msgs_per_seed + (1 if i < remainder else 0)
        seeds_with_counts.append((i, count))

    with TestPipeline() as p:
        messages = (
            p
            | beam.Create(seeds_with_counts)
            | beam.FlatMap(lambda sc, size=500: generate_batch(sc[0], sc[1], size))
        )
        count = messages | beam.combiners.Count.Globally()
        assert_that(count, equal_to([total]), label="CheckTotal")

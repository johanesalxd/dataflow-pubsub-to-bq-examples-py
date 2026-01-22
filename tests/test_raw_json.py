import json
import apache_beam as beam
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, is_empty
from dataflow_pubsub_to_bq.transforms.raw_json import ParsePubSubMessageToRawJson


# Define a custom matcher for DLQ content since timestamps/stacktraces vary
def matches_dlq_error(expected_payload):
    def _matcher(elements):
        if len(elements) != 1:
            raise ValueError(f"Expected 1 DLQ element, got {len(elements)}")
        row = elements[0]
        if row["original_payload"] != expected_payload:
            raise ValueError(
                f"Expected payload '{expected_payload}', got '{row['original_payload']}'"
            )
        if not row.get("error_message"):
            raise ValueError("DLQ row missing 'error_message'")
        if not row.get("stack_trace"):
            raise ValueError("DLQ row missing 'stack_trace'")
        if not row.get("processing_time"):
            raise ValueError("DLQ row missing 'processing_time'")

    return _matcher


def test_process_valid_message():
    """Test that valid JSON messages go to the main output."""
    subscription = "projects/test/subscriptions/sub"
    payload = {"ride_id": "123", "passenger_count": 1}
    message = PubsubMessage(
        data=json.dumps(payload).encode("utf-8"), attributes={"source": "test"}
    )

    with TestPipeline() as p:
        results = (
            p
            | beam.Create([message])
            | beam.ParDo(ParsePubSubMessageToRawJson(subscription)).with_outputs(
                "dlq", main="success"
            )
        )

        # Check success output
        assert_that(
            results.success,
            lambda elements: len(elements) == 1
            and json.loads(elements[0]["payload"]) == payload
            and elements[0]["subscription_name"] == subscription,
            label="CheckSuccess",
        )

        # Check DLQ is empty
        assert_that(results.dlq, is_empty(), label="CheckDLQEmpty")


def test_process_malformed_message():
    """Test that malformed JSON messages go to the DLQ output."""
    subscription = "projects/test/subscriptions/sub"
    invalid_json = '{"ride_id": "123", "passeng'  # truncated
    message = PubsubMessage(data=invalid_json.encode("utf-8"), attributes={})

    with TestPipeline() as p:
        results = (
            p
            | beam.Create([message])
            | beam.ParDo(ParsePubSubMessageToRawJson(subscription)).with_outputs(
                "dlq", main="success"
            )
        )

        # Check success output is empty
        assert_that(results.success, is_empty(), label="CheckSuccessEmpty")

        # Check DLQ output
        assert_that(results.dlq, matches_dlq_error(invalid_json), label="CheckDLQ")


def test_process_preserves_raw_formatting():
    """Test that valid JSON messages preserve original formatting (whitespace)."""
    subscription = "projects/test/subscriptions/sub"
    # JSON with irregular whitespace (valid)
    raw_payload = '{"ride_id":   "123", "passenger_count":\n 1}'
    message = PubsubMessage(
        data=raw_payload.encode("utf-8"), attributes={"source": "test"}
    )

    with TestPipeline() as p:
        results = (
            p
            | beam.Create([message])
            | beam.ParDo(ParsePubSubMessageToRawJson(subscription)).with_outputs(
                "dlq", main="success"
            )
        )

        # Check success output matches RAW string exactly (no normalization)
        assert_that(
            results.success,
            lambda elements: len(elements) == 1
            and elements[0]["payload"] == raw_payload,
            label="CheckRawPreservation",
        )

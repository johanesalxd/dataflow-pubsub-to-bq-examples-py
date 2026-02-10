"""Tests for the schema-driven transform and Avro-to-BigQuery type mapper."""

import json

import apache_beam as beam
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that

from dataflow_pubsub_to_bq.transforms.schema_driven_to_tablerow import (
    ParseSchemaDrivenMessage,
    avro_to_bq_schema,
    get_envelope_bigquery_schema,
)

# --- Test fixtures: inline Avro schemas (not from registry) ---

V1_SCHEMA = json.dumps(
    {
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
                "type": {"type": "long", "logicalType": "timestamp-millis"},
            },
            {"name": "meter_reading", "type": "double"},
            {"name": "meter_increment", "type": "double"},
            {"name": "ride_status", "type": "string"},
            {"name": "passenger_count", "type": "int"},
        ],
    }
)

V2_SCHEMA = json.dumps(
    {
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
                "type": {"type": "long", "logicalType": "timestamp-millis"},
            },
            {"name": "meter_reading", "type": "double"},
            {"name": "meter_increment", "type": "double"},
            {"name": "ride_status", "type": "string"},
            {"name": "passenger_count", "type": "int"},
            {
                "name": "enrichment_timestamp",
                "type": [
                    "null",
                    {"type": "long", "logicalType": "timestamp-millis"},
                ],
                "default": None,
            },
            {"name": "region", "type": ["null", "string"], "default": None},
        ],
    }
)


# --- Type mapper tests ---


def test_avro_to_bq_schema_v1():
    """Tests that v1 Avro schema produces 9 fields with correct BQ types."""
    bq_fields, field_names = avro_to_bq_schema(V1_SCHEMA)

    assert len(bq_fields) == 9
    assert len(field_names) == 9
    assert field_names == [
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

    # Check type mappings
    field_map = {f["name"]: f for f in bq_fields}
    assert field_map["ride_id"]["type"] == "STRING"
    assert field_map["point_idx"]["type"] == "INT64"
    assert field_map["latitude"]["type"] == "FLOAT64"
    assert field_map["longitude"]["type"] == "FLOAT64"
    assert field_map["timestamp"]["type"] == "TIMESTAMP"
    assert field_map["meter_reading"]["type"] == "FLOAT64"
    assert field_map["meter_increment"]["type"] == "FLOAT64"
    assert field_map["ride_status"]["type"] == "STRING"
    assert field_map["passenger_count"]["type"] == "INT64"


def test_avro_to_bq_schema_v2():
    """Tests that v2 Avro schema produces 11 fields including nullable types."""
    bq_fields, field_names = avro_to_bq_schema(V2_SCHEMA)

    assert len(bq_fields) == 11
    assert len(field_names) == 11
    assert "enrichment_timestamp" in field_names
    assert "region" in field_names

    field_map = {f["name"]: f for f in bq_fields}

    # v2 nullable fields
    assert field_map["enrichment_timestamp"]["type"] == "TIMESTAMP"
    assert field_map["enrichment_timestamp"]["mode"] == "NULLABLE"
    assert field_map["region"]["type"] == "STRING"
    assert field_map["region"]["mode"] == "NULLABLE"


def test_avro_to_bq_schema_logical_types():
    """Tests that Avro logical types map correctly to BQ types."""
    schema = json.dumps(
        {
            "type": "record",
            "name": "Test",
            "fields": [
                {
                    "name": "ts_millis",
                    "type": {
                        "type": "long",
                        "logicalType": "timestamp-millis",
                    },
                },
                {
                    "name": "ts_micros",
                    "type": {
                        "type": "long",
                        "logicalType": "timestamp-micros",
                    },
                },
                {
                    "name": "d",
                    "type": {"type": "int", "logicalType": "date"},
                },
            ],
        }
    )
    bq_fields, _ = avro_to_bq_schema(schema)
    field_map = {f["name"]: f for f in bq_fields}

    assert field_map["ts_millis"]["type"] == "TIMESTAMP"
    assert field_map["ts_micros"]["type"] == "TIMESTAMP"
    assert field_map["d"]["type"] == "DATE"


def test_avro_to_bq_schema_nullable_union():
    """Tests that union types [null, T] map to NULLABLE mode."""
    schema = json.dumps(
        {
            "type": "record",
            "name": "Test",
            "fields": [
                {
                    "name": "optional_str",
                    "type": ["null", "string"],
                    "default": None,
                },
                {
                    "name": "optional_int",
                    "type": ["null", "int"],
                    "default": None,
                },
            ],
        }
    )
    bq_fields, _ = avro_to_bq_schema(schema)
    field_map = {f["name"]: f for f in bq_fields}

    assert field_map["optional_str"]["type"] == "STRING"
    assert field_map["optional_str"]["mode"] == "NULLABLE"
    assert field_map["optional_int"]["type"] == "INT64"
    assert field_map["optional_int"]["mode"] == "NULLABLE"


def test_envelope_schema():
    """Tests that the envelope schema has 8 static fields."""
    envelope = get_envelope_bigquery_schema()
    assert len(envelope) == 8
    names = [f["name"] for f in envelope]
    assert "subscription_name" in names
    assert "message_id" in names
    assert "publish_time" in names
    assert "processing_time" in names
    assert "attributes" in names
    assert "schema_name" in names
    assert "schema_revision_id" in names
    assert "schema_encoding" in names


# --- DoFn tests ---


def test_parse_valid_v1_message():
    """Tests that a valid v1 message produces correct output with all fields."""
    payload = {
        "ride_id": "abc-123",
        "point_idx": 42,
        "latitude": 40.7128,
        "longitude": -74.0060,
        "timestamp": 1736899549168,
        "meter_reading": 15.5,
        "meter_increment": 0.25,
        "ride_status": "enroute",
        "passenger_count": 2,
    }
    message = PubsubMessage(
        data=json.dumps(payload).encode("utf-8"),
        attributes={
            "googclient_schemaname": "projects/test/schemas/taxi-ride-schema",
            "googclient_schemarevisionid": "abcd1234",
            "googclient_schemaencoding": "JSON",
        },
    )

    _, field_names = avro_to_bq_schema(V1_SCHEMA)

    with TestPipeline() as p:
        results = (
            p
            | beam.Create([message])
            | beam.ParDo(ParseSchemaDrivenMessage("test_sub", field_names))
        )

        def check_output(elements):
            assert len(elements) == 1
            row = elements[0]
            # Envelope fields
            assert row["subscription_name"] == "test_sub"
            assert row["schema_name"] == "projects/test/schemas/taxi-ride-schema"
            assert row["schema_revision_id"] == "abcd1234"
            assert row["schema_encoding"] == "JSON"
            # Payload fields
            assert row["ride_id"] == "abc-123"
            assert row["point_idx"] == 42
            assert row["latitude"] == 40.7128
            assert row["passenger_count"] == 2
            assert row["timestamp"] == 1736899549168

        assert_that(results, check_output)


def test_parse_message_with_missing_optional_fields():
    """Tests that v1 messages processed with v2 schema get None for new fields."""
    # v1 payload (missing enrichment_timestamp and region)
    payload = {
        "ride_id": "abc-123",
        "point_idx": 1,
        "latitude": 40.75,
        "longitude": -74.0,
        "timestamp": 1736899549168,
        "meter_reading": 5.0,
        "meter_increment": 0.1,
        "ride_status": "pickup",
        "passenger_count": 1,
    }
    message = PubsubMessage(
        data=json.dumps(payload).encode("utf-8"),
        attributes={},
    )

    # Use v2 field names (11 fields)
    _, field_names = avro_to_bq_schema(V2_SCHEMA)

    with TestPipeline() as p:
        results = (
            p
            | beam.Create([message])
            | beam.ParDo(ParseSchemaDrivenMessage("test_sub", field_names))
        )

        def check_output(elements):
            assert len(elements) == 1
            row = elements[0]
            # v1 fields are populated
            assert row["ride_id"] == "abc-123"
            # v2 fields are None (not in payload)
            assert row["enrichment_timestamp"] is None
            assert row["region"] is None

        assert_that(results, check_output)


def test_parse_only_extracts_schema_fields():
    """Tests that extra fields in the payload are ignored."""
    payload = {
        "ride_id": "abc-123",
        "point_idx": 1,
        "latitude": 40.75,
        "longitude": -74.0,
        "timestamp": 1736899549168,
        "meter_reading": 5.0,
        "meter_increment": 0.1,
        "ride_status": "pickup",
        "passenger_count": 1,
        "unexpected_field": "should_be_ignored",
    }
    message = PubsubMessage(
        data=json.dumps(payload).encode("utf-8"),
        attributes={},
    )

    _, field_names = avro_to_bq_schema(V1_SCHEMA)

    with TestPipeline() as p:
        results = (
            p
            | beam.Create([message])
            | beam.ParDo(ParseSchemaDrivenMessage("test_sub", field_names))
        )

        def check_output(elements):
            assert len(elements) == 1
            row = elements[0]
            assert "unexpected_field" not in row
            assert row["ride_id"] == "abc-123"

        assert_that(results, check_output)

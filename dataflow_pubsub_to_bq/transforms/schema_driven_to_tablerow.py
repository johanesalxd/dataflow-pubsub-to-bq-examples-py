"""Schema-driven transform for parsing Pub/Sub messages to BigQuery rows.

This module provides:
- Avro-to-BigQuery type mapping (avro_to_bq_schema)
- Static envelope schema for Pub/Sub metadata and schema governance fields
- A DoFn that dynamically extracts payload fields based on the Avro schema
"""

import json
import time
from typing import Any

import apache_beam as beam
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.utils.timestamp import Timestamp

# Avro primitive type to BigQuery type mapping
_AVRO_PRIMITIVE_TO_BQ: dict[str, str] = {
    "string": "STRING",
    "int": "INT64",
    "long": "INT64",
    "float": "FLOAT64",
    "double": "FLOAT64",
    "boolean": "BOOL",
    "bytes": "BYTES",
}

# Avro logical type to BigQuery type mapping
_AVRO_LOGICAL_TO_BQ: dict[str, str] = {
    "timestamp-millis": "TIMESTAMP",
    "timestamp-micros": "TIMESTAMP",
    "date": "DATE",
    "time-millis": "TIME",
    "time-micros": "TIME",
}


def _resolve_avro_type(avro_type: str | dict) -> str:
    """Resolves a single Avro type (not a union) to a BigQuery type.

    Args:
        avro_type: An Avro type definition. Either a string (primitive)
            or a dict (logical type like {"type": "long", "logicalType": "timestamp-millis"}).

    Returns:
        The corresponding BigQuery type string.

    Raises:
        ValueError: If the Avro type is not supported.
    """
    if isinstance(avro_type, str):
        bq_type = _AVRO_PRIMITIVE_TO_BQ.get(avro_type)
        if bq_type is None:
            raise ValueError(f"Unsupported Avro primitive type: {avro_type}")
        return bq_type

    if isinstance(avro_type, dict):
        logical_type = avro_type.get("logicalType")
        if logical_type:
            bq_type = _AVRO_LOGICAL_TO_BQ.get(logical_type)
            if bq_type is None:
                raise ValueError(f"Unsupported Avro logical type: {logical_type}")
            return bq_type
        # Dict without logicalType -- resolve the base type
        base_type = avro_type.get("type")
        if base_type and isinstance(base_type, str):
            return _resolve_avro_type(base_type)

    raise ValueError(f"Unsupported Avro type definition: {avro_type}")


def avro_to_bq_schema(
    avro_definition: str,
) -> tuple[list[dict[str, str]], list[str], set[str]]:
    """Parses an Avro schema JSON string and generates BigQuery schema fields.

    Args:
        avro_definition: JSON string of the Avro schema definition.

    Returns:
        A tuple of (bq_fields, field_names, timestamp_fields) where:
        - bq_fields: List of BigQuery schema field dicts with name, type, mode.
        - field_names: List of field name strings (in schema order).
        - timestamp_fields: Set of field names that are TIMESTAMP type.
            These need conversion from epoch millis/micros to Beam Timestamp
            objects for Storage Write API compatibility.

    Raises:
        ValueError: If the Avro schema contains unsupported types.
    """
    schema = json.loads(avro_definition)
    bq_fields: list[dict[str, str]] = []
    field_names: list[str] = []
    timestamp_fields: set[str] = set()

    for field in schema["fields"]:
        name = field["name"]
        avro_type = field["type"]
        field_names.append(name)

        # Handle union types: ["null", T] -> NULLABLE
        if isinstance(avro_type, list):
            non_null_types = [t for t in avro_type if t != "null"]
            if len(non_null_types) != 1:
                raise ValueError(
                    f"Unsupported union type for field '{name}': {avro_type}. "
                    "Only unions of [null, T] are supported."
                )
            bq_type = _resolve_avro_type(non_null_types[0])
            bq_fields.append({"name": name, "type": bq_type, "mode": "NULLABLE"})
        else:
            bq_type = _resolve_avro_type(avro_type)
            bq_fields.append({"name": name, "type": bq_type, "mode": "NULLABLE"})

        if bq_type == "TIMESTAMP":
            timestamp_fields.add(name)

    return bq_fields, field_names, timestamp_fields


def get_envelope_bigquery_schema() -> list[dict[str, str]]:
    """Returns the static envelope BigQuery schema fields.

    Envelope fields contain Pub/Sub metadata and schema governance attributes.
    These fields are always present regardless of the Avro schema version.

    Returns:
        List of BigQuery schema field definitions for envelope columns.
    """
    return [
        {"name": "subscription_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "message_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "publish_time", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "processing_time", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "attributes", "type": "STRING", "mode": "NULLABLE"},
        {"name": "schema_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "schema_revision_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "schema_encoding", "type": "STRING", "mode": "NULLABLE"},
    ]


class ParseSchemaDrivenMessage(beam.DoFn):
    """Parses Pub/Sub messages using schema-driven field extraction.

    This DoFn extracts:
    - Static envelope fields: subscription metadata and schema governance attributes.
    - Dynamic payload fields: extracted based on field names from the Avro schema.

    No DLQ routing is needed because the Pub/Sub Schema Registry validates
    messages at publish time. Every message that reaches this DoFn is guaranteed
    to conform to the schema.
    """

    def __init__(
        self,
        subscription_name: str,
        payload_field_names: list[str],
        timestamp_fields: set[str] | None = None,
    ):
        """Initializes the transform.

        Args:
            subscription_name: Short subscription name for BigQuery metadata.
            payload_field_names: List of field names from the Avro schema.
                These are extracted dynamically from each message payload.
            timestamp_fields: Set of field names that are TIMESTAMP type.
                Values for these fields are converted from epoch milliseconds
                to Beam Timestamp objects for Storage Write API compatibility.
        """
        self.subscription_name = subscription_name
        self.payload_field_names = payload_field_names
        self.timestamp_fields = timestamp_fields or set()

    def process(self, element: PubsubMessage) -> Any:
        """Processes a single Pub/Sub message.

        Args:
            element: PubsubMessage containing data and attributes.

        Yields:
            Dictionary with envelope + dynamic payload fields for BigQuery.
        """
        # Decode and parse JSON payload
        message_data = element.data.decode("utf-8")
        payload = json.loads(message_data)

        # Extract Pub/Sub metadata
        message_id = element.message_id if hasattr(element, "message_id") else ""

        if hasattr(element, "publish_time") and element.publish_time:
            publish_time = Timestamp.of(element.publish_time.timestamp())
        else:
            publish_time = Timestamp.of(time.time())

        processing_time = Timestamp.of(time.time())

        attributes = element.attributes if element.attributes else {}

        # Extract schema governance attributes
        schema_name = attributes.get("googclient_schemaname", "")
        schema_revision_id = attributes.get("googclient_schemarevisionid", "")
        schema_encoding = attributes.get("googclient_schemaencoding", "")

        # Build envelope fields
        bq_row: dict[str, Any] = {
            "subscription_name": self.subscription_name,
            "message_id": message_id,
            "publish_time": publish_time,
            "processing_time": processing_time,
            "attributes": json.dumps(attributes),
            "schema_name": schema_name,
            "schema_revision_id": schema_revision_id,
            "schema_encoding": schema_encoding,
        }

        # Dynamic payload fields -- driven by schema
        for field_name in self.payload_field_names:
            value = payload.get(field_name)
            # Convert epoch millis to Beam Timestamp for TIMESTAMP columns
            if value is not None and field_name in self.timestamp_fields:
                value = Timestamp.of(value / 1000.0)
            bq_row[field_name] = value

        yield bq_row

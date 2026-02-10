"""Generates a BigQuery schema string from the Pub/Sub Schema Registry.

Fetches the Avro schema from the registry using the SchemaServiceClient SDK,
maps it to BigQuery types using the schema-driven transform, and outputs a
bq CLI compatible schema string (name:TYPE,name:TYPE,...) to stdout.

Diagnostic messages go to stderr so the shell script can capture only the
schema string.

Usage:
    python schemas/generate_bq_schema.py \
        --schema=projects/PROJECT/schemas/SCHEMA_NAME
"""

import argparse
import logging
import sys

from google.cloud.pubsub_v1 import SchemaServiceClient

from dataflow_pubsub_to_bq.transforms.schema_driven_to_tablerow import (
    avro_to_bq_schema,
    get_envelope_bigquery_schema,
)

logger = logging.getLogger(__name__)


def generate_bq_schema_string(schema_resource_path: str) -> str:
    """Fetches an Avro schema from the registry and returns a bq CLI schema string.

    Args:
        schema_resource_path: Full Pub/Sub schema resource path
            (e.g., projects/my-project/schemas/my-schema).

    Returns:
        Comma-separated bq CLI schema string (e.g., "name:STRING,count:INT64").
    """
    client = SchemaServiceClient()
    schema = client.get_schema(request={"name": schema_resource_path})
    logger.info("Schema revision: %s", schema.revision_id)

    payload_fields, field_names = avro_to_bq_schema(schema.definition)
    logger.info("Payload fields (%d): %s", len(field_names), field_names)

    envelope_fields = get_envelope_bigquery_schema()
    all_fields = envelope_fields + payload_fields

    parts = [f"{f['name']}:{f['type']}" for f in all_fields]
    return ",".join(parts)


def main() -> None:
    """Entry point for the BQ schema generator."""
    parser = argparse.ArgumentParser(
        description="Generate BigQuery schema from Pub/Sub Schema Registry"
    )
    parser.add_argument(
        "--schema",
        required=True,
        help="Full Pub/Sub schema resource path (e.g., projects/PROJECT/schemas/NAME)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s: %(message)s",
        stream=sys.stderr,
    )

    bq_schema = generate_bq_schema_string(args.schema)
    print(bq_schema)


if __name__ == "__main__":
    main()

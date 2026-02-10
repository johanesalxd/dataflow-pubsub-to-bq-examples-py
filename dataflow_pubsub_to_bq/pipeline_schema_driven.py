"""Schema-driven pipeline for reading from Pub/Sub and writing to BigQuery.

This pipeline fetches the Avro schema from the Pub/Sub Schema Registry at
startup and uses it to dynamically generate both the BigQuery table schema
and the field extraction logic. No hardcoded field lists -- the schema
registry is the single source of truth.

No DLQ is needed because the schema registry validates messages at publish
time. Every message that reaches this pipeline conforms to the schema.
"""

import logging

import apache_beam as beam
from apache_beam.io.gcp import bigquery
from apache_beam.io.gcp import pubsub
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud.pubsub_v1 import SchemaServiceClient

from dataflow_pubsub_to_bq.pipeline_schema_driven_options import (
    SchemaDrivenOptions,
)
from dataflow_pubsub_to_bq.transforms.schema_driven_to_tablerow import (
    ParseSchemaDrivenMessage,
    avro_to_bq_schema,
    get_envelope_bigquery_schema,
)


def run(argv=None):
    """Runs the schema-driven Pub/Sub to BigQuery pipeline.

    Args:
        argv: Command-line arguments.
    """
    # Parse pipeline options
    pipeline_options = PipelineOptions(argv)
    custom_options = pipeline_options.view_as(SchemaDrivenOptions)

    # Fetch Avro schema from the Pub/Sub Schema Registry
    logging.info("Fetching schema from: %s", custom_options.pubsub_schema)
    schema_client = SchemaServiceClient()
    schema = schema_client.get_schema(request={"name": custom_options.pubsub_schema})
    logging.info("Schema revision: %s", schema.revision_id)

    # Parse Avro schema to generate BQ fields and field names
    payload_bq_fields, payload_field_names = avro_to_bq_schema(schema.definition)
    logging.info(
        "Payload fields from schema (%d): %s",
        len(payload_field_names),
        payload_field_names,
    )

    # Combine envelope (static) + payload (dynamic) fields
    full_bq_schema = get_envelope_bigquery_schema() + payload_bq_fields

    # Create the pipeline
    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Read from Pub/Sub
        messages = pipeline | "ReadFromPubSub" >> pubsub.ReadFromPubSub(
            subscription=custom_options.subscription,
            with_attributes=True,
        )

        # Parse messages and write to BigQuery
        (
            messages
            | "ParseMessages"
            >> beam.ParDo(
                ParseSchemaDrivenMessage(
                    custom_options.subscription_name,
                    payload_field_names,
                )
            )
            | "WriteToBigQuery"
            >> bigquery.WriteToBigQuery(
                table=custom_options.output_table,
                schema={"fields": full_bq_schema},
                write_disposition=bigquery.BigQueryDisposition.WRITE_APPEND,
                create_disposition=bigquery.BigQueryDisposition.CREATE_NEVER,
                method=bigquery.WriteToBigQuery.Method.STORAGE_WRITE_API,
                triggering_frequency=1,
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()

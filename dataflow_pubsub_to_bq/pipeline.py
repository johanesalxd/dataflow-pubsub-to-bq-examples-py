"""Main pipeline for reading from Pub/Sub and writing to BigQuery.

This pipeline reads taxi ride data from a Pub/Sub subscription,
parses the JSON messages, and writes them to a BigQuery table.
"""

import logging

import apache_beam as beam
from apache_beam.io.gcp import bigquery
from apache_beam.io.gcp import pubsub
from apache_beam.options.pipeline_options import PipelineOptions
from dataflow_pubsub_to_bq.pipeline_options import PubSubToBigQueryOptions
from dataflow_pubsub_to_bq.transforms.json_to_tablerow import get_bigquery_schema
from dataflow_pubsub_to_bq.transforms.json_to_tablerow import ParsePubSubMessage
from dataflow_pubsub_to_bq.transforms.raw_json import get_dead_letter_bigquery_schema


def run(argv=None):
    """Runs the Pub/Sub to BigQuery pipeline.

    Args:
        argv: Command-line arguments.
    """
    # Parse pipeline options
    pipeline_options = PipelineOptions(argv)
    custom_options = pipeline_options.view_as(PubSubToBigQueryOptions)

    # Create the pipeline
    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Read from Pub/Sub
        messages = pipeline | "ReadFromPubSub" >> pubsub.ReadFromPubSub(
            subscription=custom_options.subscription,
            with_attributes=True,
        )

        # Parse messages and write to BigQuery with Storage Write API
        rows = (
            messages
            | "ParseMessages"
            >> beam.ParDo(ParsePubSubMessage(custom_options.subscription_name))
            .with_outputs("dlq", main="success")
        )

        (
            rows.success
            | "WriteToBigQuery"
            >> bigquery.WriteToBigQuery(
                table=custom_options.output_table,
                schema={"fields": get_bigquery_schema()},
                write_disposition=bigquery.BigQueryDisposition.WRITE_APPEND,
                create_disposition=bigquery.BigQueryDisposition.CREATE_NEVER,
                method=bigquery.WriteToBigQuery.Method.STORAGE_WRITE_API,
                triggering_frequency=1,
            )
        )

        # Write Failures to BigQuery DLQ
        (
            rows.dlq
            | "WriteFailuresToBigQuery"
            >> bigquery.WriteToBigQuery(
                table=f"{custom_options.output_table}_dlq",
                schema={"fields": get_dead_letter_bigquery_schema()},
                write_disposition=bigquery.BigQueryDisposition.WRITE_APPEND,
                create_disposition=bigquery.BigQueryDisposition.CREATE_NEVER,
                method=bigquery.WriteToBigQuery.Method.STORAGE_WRITE_API,
                triggering_frequency=1,
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()

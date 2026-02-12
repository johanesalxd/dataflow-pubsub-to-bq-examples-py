"""Main pipeline for reading from Pub/Sub and writing to BigQuery (JSON Column).

This pipeline reads taxi ride data from a Pub/Sub subscription,
and writes the raw JSON payload to a BigQuery JSON column.
"""

import logging

import apache_beam as beam
from apache_beam.io.gcp import bigquery
from apache_beam.io.gcp import pubsub
from apache_beam.options.pipeline_options import PipelineOptions
from dataflow_pubsub_to_bq.pipeline_options import PubSubToBigQueryOptions
from dataflow_pubsub_to_bq.transforms.raw_json import get_dead_letter_bigquery_schema
from dataflow_pubsub_to_bq.transforms.raw_json import get_raw_json_bigquery_schema
from dataflow_pubsub_to_bq.transforms.raw_json import ParsePubSubMessageToRawJson


def run(argv=None):
    """Runs the Pub/Sub to BigQuery pipeline (JSON version).

    Args:
        argv: Command-line arguments.
    """
    # Parse pipeline options
    pipeline_options = PipelineOptions(argv)
    custom_options = pipeline_options.view_as(PubSubToBigQueryOptions)

    use_at_least_once = custom_options.use_at_least_once
    logging.info("BigQuery use_at_least_once: %s", use_at_least_once)

    # Create the pipeline
    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Read from Pub/Sub
        messages = pipeline | "ReadFromPubSub" >> pubsub.ReadFromPubSub(
            subscription=custom_options.subscription,
            with_attributes=True,
        )

        # Parse messages
        rows = messages | "ParseMessagesToRawJson" >> beam.ParDo(
            ParsePubSubMessageToRawJson(custom_options.subscription_name)
        ).with_outputs("dlq", main="success")

        # Write Success to BigQuery with Storage Write API
        (
            rows.success
            | "WriteToBigQuery"
            >> bigquery.WriteToBigQuery(
                table=custom_options.output_table,
                schema={"fields": get_raw_json_bigquery_schema()},
                write_disposition=bigquery.BigQueryDisposition.WRITE_APPEND,
                create_disposition=bigquery.BigQueryDisposition.CREATE_NEVER,
                method=bigquery.WriteToBigQuery.Method.STORAGE_WRITE_API,
                use_at_least_once=use_at_least_once,
                triggering_frequency=1,
            )
        )

        # Write Failures to BigQuery DLQ
        (
            rows.dlq
            | "WriteToDLQ"
            >> bigquery.WriteToBigQuery(
                table=f"{custom_options.output_table}_dlq",
                schema={"fields": get_dead_letter_bigquery_schema()},
                write_disposition=bigquery.BigQueryDisposition.WRITE_APPEND,
                create_disposition=bigquery.BigQueryDisposition.CREATE_NEVER,
                method=bigquery.WriteToBigQuery.Method.STORAGE_WRITE_API,
                use_at_least_once=use_at_least_once,
                triggering_frequency=1,
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()

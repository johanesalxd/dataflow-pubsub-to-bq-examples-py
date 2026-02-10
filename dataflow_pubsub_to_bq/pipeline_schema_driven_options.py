"""Custom pipeline options for the schema-driven Pub/Sub to BigQuery pipeline."""

from apache_beam.options.pipeline_options import PipelineOptions


class SchemaDrivenOptions(PipelineOptions):
    """Pipeline options for the schema-driven pipeline.

    Extends the base options with schema registry configuration.
    The pipeline fetches the Avro schema from the Pub/Sub Schema Registry
    at startup and uses it to drive BQ schema generation and field extraction.
    """

    @classmethod
    def _add_argparse_args(cls, parser):
        """Adds custom command-line arguments to the parser.

        Args:
            parser: ArgumentParser object to add arguments to.
        """
        parser.add_argument(
            "--subscription",
            required=True,
            help="Pub/Sub subscription path (e.g., projects/PROJECT/subscriptions/SUB)",
        )
        parser.add_argument(
            "--output_table",
            required=True,
            help="BigQuery output table (e.g., PROJECT:DATASET.TABLE)",
        )
        parser.add_argument(
            "--subscription_name",
            required=True,
            help="Short subscription name to record in BigQuery metadata",
        )
        parser.add_argument(
            "--pubsub_schema",
            required=True,
            help="Pub/Sub schema resource path (e.g., projects/PROJECT/schemas/SCHEMA)",
        )

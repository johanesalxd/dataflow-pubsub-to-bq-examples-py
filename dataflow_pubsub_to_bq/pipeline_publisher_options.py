"""Custom pipeline options for the synthetic data publisher pipeline."""

from apache_beam.options.pipeline_options import PipelineOptions


class PublisherPipelineOptions(PipelineOptions):
    """Custom pipeline options for the Dataflow publisher pipeline.

    These options define the configuration for generating synthetic
    messages and publishing them to a Pub/Sub topic.
    """

    @classmethod
    def _add_argparse_args(cls, parser):
        """Adds custom command-line arguments to the parser.

        Args:
            parser: ArgumentParser object to add arguments to.
        """
        parser.add_argument(
            "--topic",
            default=None,
            help="Full Pub/Sub topic path "
            "(e.g., projects/PROJECT_ID/topics/TOPIC_NAME).",
        )
        parser.add_argument(
            "--num_messages",
            type=int,
            default=36_000_000,
            help="Total number of messages to publish (default: 36000000, "
            "~1 hour at 100 MB/s with 10 KB messages).",
        )
        parser.add_argument(
            "--message_size_bytes",
            type=int,
            default=10_000,
            help="Target size per message in bytes (default: 10000).",
        )
        parser.add_argument(
            "--format",
            type=str,
            default="json",
            choices=["json", "avro"],
            help="Message serialization format. json=padded JSON (default), "
            "avro=Avro binary (~85 bytes, no padding). When avro is selected, "
            "--message_size_bytes is ignored.",
        )
        parser.add_argument(
            "--validate_only",
            action="store_true",
            default=False,
            help="Validate message sizes and rate calculations without "
            "launching a pipeline or publishing anything.",
        )

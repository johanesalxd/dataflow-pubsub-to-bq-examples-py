"""Mirror publisher that bridges an unschematized Pub/Sub topic to a schema-governed topic.

Reads messages from a source subscription (e.g., on the public taxi topic),
converts the timestamp field from ISO 8601 string to epoch milliseconds
(to match the Avro timestamp-millis logical type), and re-publishes to a
schema-enabled topic. Pub/Sub validates each message against the Avro schema
at publish time.

Usage:
    python publish_to_schema_topic.py \
        --project=my-project \
        --source-subscription=projects/my-project/subscriptions/taxi_telemetry_schema_source \
        --target-topic=projects/my-project/topics/taxi_telemetry_schema
"""

import argparse
import json
import logging
import signal
import sys
from datetime import datetime
from concurrent.futures import Future

from google.cloud import pubsub_v1

logger = logging.getLogger(__name__)


def convert_timestamp_to_millis(payload: dict) -> dict:
    """Converts the timestamp field from ISO 8601 string to epoch milliseconds.

    Args:
        payload: Parsed JSON payload dict.

    Returns:
        The payload with the timestamp field converted to epoch milliseconds.
        If the timestamp field is missing or cannot be parsed, the payload
        is returned unchanged.
    """
    timestamp_str = payload.get("timestamp")
    if timestamp_str and isinstance(timestamp_str, str):
        try:
            dt = datetime.fromisoformat(timestamp_str)
            payload["timestamp"] = int(dt.timestamp() * 1000)
        except (ValueError, AttributeError):
            logger.warning("Failed to parse timestamp: %s", timestamp_str)
    return payload


def publish_callback(future: Future, message_data: str) -> None:
    """Callback for handling publish results.

    Args:
        future: The publish future.
        message_data: The original message data for logging.
    """
    try:
        future.result(timeout=30)
    except Exception as e:
        logger.error("Publish failed: %s (data: %s...)", e, message_data[:100])


def run(
    project: str,
    source_subscription: str,
    target_topic: str,
) -> None:
    """Runs the mirror publisher.

    Args:
        project: GCP project ID.
        source_subscription: Full subscription path to read from.
        target_topic: Full topic path to publish to.
    """
    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()

    published_count = 0
    error_count = 0

    def callback(message: pubsub_v1.subscriber.message.Message) -> None:
        """Processes a single message from the source subscription."""
        nonlocal published_count, error_count
        try:
            # Parse and convert
            payload = json.loads(message.data.decode("utf-8"))
            converted = convert_timestamp_to_millis(payload)
            data = json.dumps(converted).encode("utf-8")

            # Publish to schema topic
            future = publisher.publish(target_topic, data=data)
            future.add_done_callback(
                lambda f: publish_callback(f, data.decode("utf-8"))
            )

            published_count += 1
            if published_count % 1000 == 0:
                logger.info(
                    "Published %d messages (%d errors)",
                    published_count,
                    error_count,
                )

            message.ack()

        except Exception as e:
            error_count += 1
            logger.error("Processing failed: %s", e)
            message.nack()

    # Start streaming pull
    streaming_pull_future = subscriber.subscribe(source_subscription, callback=callback)
    logger.info(
        "Listening on %s, publishing to %s",
        source_subscription,
        target_topic,
    )

    # Graceful shutdown on SIGINT/SIGTERM
    def shutdown(signum, frame):
        logger.info(
            "Shutting down (published=%d, errors=%d)...",
            published_count,
            error_count,
        )
        streaming_pull_future.cancel()
        subscriber.close()
        publisher.transport.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        streaming_pull_future.result()
    except Exception:
        streaming_pull_future.cancel()
        subscriber.close()


def main():
    """Entry point for the mirror publisher."""
    parser = argparse.ArgumentParser(
        description="Mirror publisher for Pub/Sub schema topic"
    )
    parser.add_argument(
        "--project",
        required=True,
        help="GCP project ID",
    )
    parser.add_argument(
        "--source-subscription",
        required=True,
        help="Full source subscription path",
    )
    parser.add_argument(
        "--target-topic",
        required=True,
        help="Full target topic path (schema-enabled)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    run(
        project=args.project,
        source_subscription=args.source_subscription,
        target_topic=args.target_topic,
    )


if __name__ == "__main__":
    main()

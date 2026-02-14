package com.johanesalxd;

import com.johanesalxd.schemas.DeadLetterBigQuerySchema;
import com.johanesalxd.schemas.RawJsonBigQuerySchema;
import com.johanesalxd.transforms.PubsubMessageToRawJson;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;

public class PubSubToBigQueryJson {

  public static void main(String[] args) {
    // Parse pipeline options
    PubSubToBigQueryOptions options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(PubSubToBigQueryOptions.class);

    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    // Read from Pub/Sub and Parse
    PCollectionTuple results = pipeline
        .apply("ReadFromPubSub", PubsubIO.readMessagesWithAttributes()
            .fromSubscription(options.getSubscription()))
        .apply("ParseMessagesToRawJson", ParDo.of(
            new PubsubMessageToRawJson(options.getSubscriptionName()))
            .withOutputTags(PubsubMessageToRawJson.SUCCESS_TAG, 
                TupleTagList.of(PubsubMessageToRawJson.DLQ_TAG)));

    // Write Success to BigQuery using at-least-once semantics (default stream).
    // This matches production config and avoids CreateWriteStream quota pressure.
    // Connection pooling is enabled via --useStorageApiConnectionPool=true at launch.
    results.get(PubsubMessageToRawJson.SUCCESS_TAG)
        .apply("WriteToBigQuery", BigQueryIO.writeTableRows()
            .to(options.getOutputTable())
            .withSchema(RawJsonBigQuerySchema.getSchema())
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .withMethod(BigQueryIO.Write.Method.STORAGE_API_AT_LEAST_ONCE)
        );

    // Write Failures to BigQuery DLQ (at-least-once, low volume)
    results.get(PubsubMessageToRawJson.DLQ_TAG)
        .apply("WriteToDLQ", BigQueryIO.writeTableRows()
            .to(options.getOutputTable() + "_dlq")
            .withSchema(DeadLetterBigQuerySchema.getSchema())
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .withMethod(BigQueryIO.Write.Method.STORAGE_API_AT_LEAST_ONCE)
        );

    // Run the pipeline
    pipeline.run();
  }
}

package com.johanesalxd;

import com.google.api.services.bigquery.model.TableRow;
import com.johanesalxd.schemas.DeadLetterBigQuerySchema;
import com.johanesalxd.schemas.TaxiRideBigQuerySchema;
import com.johanesalxd.transforms.PubsubAvroToTableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;

/**
 * Streaming pipeline that reads Avro binary from Pub/Sub and writes to BigQuery
 * with dynamic table routing based on ride_status.
 *
 * <p>Architecture:
 * <pre>
 *   Pub/Sub (Avro binary)
 *     -> Deserialize Avro + extract typed fields
 *     -> Route to BigQuery tables by ride_status:
 *         {outputTableBase}_enroute
 *         {outputTableBase}_pickup
 *         {outputTableBase}_dropoff
 *         {outputTableBase}_waiting
 *     -> DLQ: {outputTableBase}_dlq
 * </pre>
 *
 * <p>Uses {@code STORAGE_API_AT_LEAST_ONCE} with connection pooling enabled,
 * matching the production pipeline configuration. The {@code KV<String, TableRow>}
 * pattern cleanly separates routing (key) from data (value) without mutating
 * elements inside the routing function.
 */
public class PubSubToBigQueryAvro {

  public static void main(String[] args) {
    // Parse pipeline options
    PubSubToBigQueryAvroOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(PubSubToBigQueryAvroOptions.class);

    // Enable Storage Write API connection pooling (multiplexing).
    // Shares connections across destination tables, reducing total connection count.
    BigQueryOptions bqOptions = options.as(BigQueryOptions.class);
    bqOptions.setUseStorageApiConnectionPool(true);

    String outputTableBase = options.getOutputTableBase();

    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    // Read from Pub/Sub and decode Avro
    PCollectionTuple results =
        pipeline
            .apply(
                "ReadFromPubSub",
                PubsubIO.readMessagesWithAttributes()
                    .fromSubscription(options.getSubscription()))
            .apply(
                "DecodeAvroAndRoute",
                ParDo.of(new PubsubAvroToTableRow(options.getSubscriptionName()))
                    .withOutputTags(
                        PubsubAvroToTableRow.SUCCESS_TAG,
                        TupleTagList.of(PubsubAvroToTableRow.DLQ_TAG)));

    // Write success to BigQuery with dynamic routing.
    // Uses BigQueryIO.<KV<String, TableRow>>write() with withFormatFunction()
    // to cleanly separate routing (key = ride_status) from data (value = TableRow).
    // The .to() function is pure -- it only reads the key, never mutates the element.
    results
        .get(PubsubAvroToTableRow.SUCCESS_TAG)
        .apply(
            "WriteToBigQuery",
            BigQueryIO.<KV<String, TableRow>>write()
                .to(
                    raw -> {
                      String rideStatus = raw.getValue().getKey();
                      return new TableDestination(
                          outputTableBase + "_" + rideStatus, null);
                    })
                .withFormatFunction(KV::getValue)
                .withSchema(TaxiRideBigQuerySchema.getSchema())
                .withWriteDisposition(
                    BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(
                    BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .withMethod(
                    BigQueryIO.Write.Method.STORAGE_API_AT_LEAST_ONCE));

    // Write failures to BigQuery DLQ (single table, at-least-once, low volume)
    results
        .get(PubsubAvroToTableRow.DLQ_TAG)
        .apply(
            "WriteToDLQ",
            BigQueryIO.writeTableRows()
                .to(outputTableBase + "_dlq")
                .withSchema(DeadLetterBigQuerySchema.getSchema())
                .withWriteDisposition(
                    BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(
                    BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withMethod(
                    BigQueryIO.Write.Method.STORAGE_API_AT_LEAST_ONCE));

    // Run the pipeline
    pipeline.run();
  }
}

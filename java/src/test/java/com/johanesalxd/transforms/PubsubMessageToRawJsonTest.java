package com.johanesalxd.transforms;

import com.google.api.services.bigquery.model.TableRow;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.Rule;
import org.junit.Test;

public class PubsubMessageToRawJsonTest {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testProcessElementSuccess() {
    String jsonContent = "{\"ride_id\": \"123\", \"passenger_count\": 1}";
    PubsubMessage message = new PubsubMessage(
        jsonContent.getBytes(StandardCharsets.UTF_8),
        Collections.singletonMap("source", "test")
    );

    PCollectionTuple results = pipeline
        .apply(Create.of(message))
        .apply(ParDo.of(new PubsubMessageToRawJson("projects/test/subscriptions/sub"))
            .withOutputTags(PubsubMessageToRawJson.SUCCESS_TAG, 
                TupleTagList.of(PubsubMessageToRawJson.DLQ_TAG)));

    PCollection<TableRow> success = results.get(PubsubMessageToRawJson.SUCCESS_TAG);
    PCollection<TableRow> dlq = results.get(PubsubMessageToRawJson.DLQ_TAG);

    PAssert.that(success).satisfies(rows -> {
      TableRow row = rows.iterator().next();
      if (!row.get("payload").equals(jsonContent)) {
        throw new AssertionError("Payload mismatch");
      }
      if (!row.get("subscription_name").equals("projects/test/subscriptions/sub")) {
        throw new AssertionError("Subscription mismatch");
      }
      // Check attribute extraction
      if (!row.get("attributes").toString().contains("\"source\":\"test\"")) {
        throw new AssertionError("Attributes mismatch");
      }
      return null;
    });

    PAssert.that(dlq).empty();

    pipeline.run();
  }

  @Test
  public void testProcessElementFailure() {
    String invalidJson = "{\"ride_id\": \"123\", \"passeng"; // Malformed
    PubsubMessage message = new PubsubMessage(
        invalidJson.getBytes(StandardCharsets.UTF_8),
        Collections.emptyMap()
    );

    PCollectionTuple results = pipeline
        .apply(Create.of(message))
        .apply(ParDo.of(new PubsubMessageToRawJson("projects/test/subscriptions/sub"))
            .withOutputTags(PubsubMessageToRawJson.SUCCESS_TAG, 
                TupleTagList.of(PubsubMessageToRawJson.DLQ_TAG)));

    PCollection<TableRow> success = results.get(PubsubMessageToRawJson.SUCCESS_TAG);
    PCollection<TableRow> dlq = results.get(PubsubMessageToRawJson.DLQ_TAG);

    PAssert.that(success).empty();

    PAssert.that(dlq).satisfies(rows -> {
      TableRow row = rows.iterator().next();
      if (!row.get("original_payload").equals(invalidJson)) {
        throw new AssertionError("Original payload mismatch");
      }
      if (row.get("error_message") == null) {
        throw new AssertionError("Error message should not be null");
      }
      if (row.get("stack_trace") == null) {
        throw new AssertionError("Stack trace should not be null");
      }
      return null;
    });

    pipeline.run();
  }

  @Test
  public void testProcessPreservesRawFormatting() {
    // JSON with irregular whitespace (valid)
    String rawPayload = "{\"ride_id\":   \"123\", \"passenger_count\":\n 1}";
    PubsubMessage message = new PubsubMessage(
        rawPayload.getBytes(StandardCharsets.UTF_8),
        Collections.singletonMap("source", "test")
    );

    PCollectionTuple results = pipeline
        .apply(Create.of(message))
        .apply(ParDo.of(new PubsubMessageToRawJson("projects/test/subscriptions/sub"))
            .withOutputTags(PubsubMessageToRawJson.SUCCESS_TAG, 
                TupleTagList.of(PubsubMessageToRawJson.DLQ_TAG)));

    PCollection<TableRow> success = results.get(PubsubMessageToRawJson.SUCCESS_TAG);

    PAssert.that(success).satisfies(rows -> {
      TableRow row = rows.iterator().next();
      // Check success output matches RAW string exactly (no normalization/re-serialization)
      if (!row.get("payload").equals(rawPayload)) {
        throw new AssertionError(
            String.format("Payload mismatch. Expected: '%s', Got: '%s'", rawPayload, row.get("payload"))
        );
      }
      return null;
    });

    pipeline.run();
  }
}

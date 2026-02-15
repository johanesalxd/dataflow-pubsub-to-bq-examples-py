package com.johanesalxd.transforms;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

import com.google.api.services.bigquery.model.TableRow;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.Rule;
import org.junit.Test;

public class PubsubAvroToTableRowTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private static final String AVRO_SCHEMA_JSON =
      "{"
          + "\"type\":\"record\","
          + "\"name\":\"TaxiRide\","
          + "\"namespace\":\"com.example.taxi\","
          + "\"fields\":["
          + "{\"name\":\"ride_id\",\"type\":\"string\"},"
          + "{\"name\":\"point_idx\",\"type\":\"int\"},"
          + "{\"name\":\"latitude\",\"type\":\"double\"},"
          + "{\"name\":\"longitude\",\"type\":\"double\"},"
          + "{\"name\":\"timestamp\",\"type\":{\"type\":\"string\","
          + "\"logicalType\":\"iso-datetime\"}},"
          + "{\"name\":\"meter_reading\",\"type\":\"double\"},"
          + "{\"name\":\"meter_increment\",\"type\":\"double\"},"
          + "{\"name\":\"ride_status\",\"type\":\"string\"},"
          + "{\"name\":\"passenger_count\",\"type\":\"int\"}"
          + "]}";

  private static final Schema AVRO_SCHEMA = new Schema.Parser().parse(AVRO_SCHEMA_JSON);

  /** Serializes a GenericRecord to schemaless Avro binary bytes. */
  private static byte[] serializeAvro(GenericRecord record) throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(AVRO_SCHEMA);
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    writer.write(record, encoder);
    encoder.flush();
    return out.toByteArray();
  }

  /** Creates a valid taxi ride GenericRecord with the given ride_status. */
  private static GenericRecord createTaxiRecord(String rideStatus) {
    GenericRecord record = new GenericData.Record(AVRO_SCHEMA);
    record.put("ride_id", "ride-123456");
    record.put("point_idx", 500);
    record.put("latitude", 1.345678);
    record.put("longitude", 103.854321);
    record.put("timestamp", "2026-02-15T08:30:00.000000+08:00");
    record.put("meter_reading", 45.5);
    record.put("meter_increment", 0.0523);
    record.put("ride_status", rideStatus);
    record.put("passenger_count", 3);
    return record;
  }

  @Test
  public void testValidAvroMessage() throws Exception {
    GenericRecord record = createTaxiRecord("enroute");
    byte[] avroBytes = serializeAvro(record);

    PubsubMessage message =
        new PubsubMessage(avroBytes, Collections.singletonMap("source", "test"));

    PCollectionTuple results =
        pipeline
            .apply(Create.of(message))
            .apply(
                ParDo.of(new PubsubAvroToTableRow("perf_test_sub_a"))
                    .withOutputTags(
                        PubsubAvroToTableRow.SUCCESS_TAG,
                        TupleTagList.of(PubsubAvroToTableRow.DLQ_TAG)));

    PCollection<KV<String, TableRow>> success =
        results.get(PubsubAvroToTableRow.SUCCESS_TAG);
    PCollection<TableRow> dlq = results.get(PubsubAvroToTableRow.DLQ_TAG);

    PAssert.that(success)
        .satisfies(
            rows -> {
              KV<String, TableRow> kv = rows.iterator().next();
              // Verify routing key
              if (!"enroute".equals(kv.getKey())) {
                throw new AssertionError(
                    "Expected routing key 'enroute', got: " + kv.getKey());
              }
              // Verify typed fields
              TableRow row = kv.getValue();
              if (!"ride-123456".equals(row.get("ride_id"))) {
                throw new AssertionError("ride_id mismatch");
              }
              if (!Integer.valueOf(500).equals(row.get("point_idx"))) {
                throw new AssertionError("point_idx mismatch");
              }
              if (!(row.get("latitude") instanceof Double)) {
                throw new AssertionError("latitude should be Double");
              }
              if (!"enroute".equals(row.get("ride_status"))) {
                throw new AssertionError("ride_status mismatch");
              }
              if (!Integer.valueOf(3).equals(row.get("passenger_count"))) {
                throw new AssertionError("passenger_count mismatch");
              }
              // Verify envelope fields
              if (!"perf_test_sub_a".equals(row.get("subscription_name"))) {
                throw new AssertionError("subscription_name mismatch");
              }
              if (row.get("publish_time") == null) {
                throw new AssertionError("publish_time should not be null");
              }
              if (row.get("processing_time") == null) {
                throw new AssertionError("processing_time should not be null");
              }
              // Verify attributes
              if (!row.get("attributes").toString().contains("\"source\":\"test\"")) {
                throw new AssertionError("attributes mismatch");
              }
              return null;
            });

    PAssert.that(dlq).empty();

    pipeline.run();
  }

  @Test
  public void testDifferentRideStatuses() throws Exception {
    String[] statuses = {"enroute", "pickup", "dropoff", "waiting"};
    List<PubsubMessage> messages = new ArrayList<>();
    for (String status : statuses) {
      GenericRecord record = createTaxiRecord(status);
      byte[] avroBytes = serializeAvro(record);
      messages.add(new PubsubMessage(avroBytes, Collections.emptyMap()));
    }

    PCollectionTuple results =
        pipeline
            .apply(Create.of(messages))
            .apply(
                ParDo.of(new PubsubAvroToTableRow("perf_test_sub_a"))
                    .withOutputTags(
                        PubsubAvroToTableRow.SUCCESS_TAG,
                        TupleTagList.of(PubsubAvroToTableRow.DLQ_TAG)));

    PCollection<KV<String, TableRow>> success =
        results.get(PubsubAvroToTableRow.SUCCESS_TAG);

    PAssert.that(success)
        .satisfies(
            rows -> {
              List<String> keys = new ArrayList<>();
              for (KV<String, TableRow> kv : rows) {
                keys.add(kv.getKey());
                // Verify routing key matches ride_status in the row
                String rowStatus = (String) kv.getValue().get("ride_status");
                if (!kv.getKey().equals(rowStatus)) {
                  throw new AssertionError(
                      "Routing key '"
                          + kv.getKey()
                          + "' does not match ride_status '"
                          + rowStatus
                          + "'");
                }
              }
              assertThat("Expected 4 outputs", keys, hasSize(4));
              for (String status : statuses) {
                if (!keys.contains(status)) {
                  throw new AssertionError("Missing routing key: " + status);
                }
              }
              return null;
            });

    pipeline.run();
  }

  @Test
  public void testMalformedAvroToDLQ() {
    byte[] garbage = "not valid avro data".getBytes(StandardCharsets.UTF_8);
    PubsubMessage message = new PubsubMessage(garbage, Collections.emptyMap());

    PCollectionTuple results =
        pipeline
            .apply(Create.of(message))
            .apply(
                ParDo.of(new PubsubAvroToTableRow("perf_test_sub_a"))
                    .withOutputTags(
                        PubsubAvroToTableRow.SUCCESS_TAG,
                        TupleTagList.of(PubsubAvroToTableRow.DLQ_TAG)));

    PCollection<KV<String, TableRow>> success =
        results.get(PubsubAvroToTableRow.SUCCESS_TAG);
    PCollection<TableRow> dlq = results.get(PubsubAvroToTableRow.DLQ_TAG);

    PAssert.that(success).empty();

    PAssert.that(dlq)
        .satisfies(
            rows -> {
              TableRow row = rows.iterator().next();
              if (row.get("error_message") == null) {
                throw new AssertionError("error_message should not be null");
              }
              if (row.get("stack_trace") == null) {
                throw new AssertionError("stack_trace should not be null");
              }
              if (!"not valid avro data".equals(row.get("original_payload"))) {
                throw new AssertionError("original_payload mismatch");
              }
              if (!"perf_test_sub_a".equals(row.get("subscription_name"))) {
                throw new AssertionError("subscription_name mismatch");
              }
              return null;
            });

    pipeline.run();
  }

  @Test
  public void testTimestampFieldPreserved() throws Exception {
    String expectedTimestamp = "2026-02-15T08:30:00.000000+08:00";
    GenericRecord record = createTaxiRecord("pickup");
    byte[] avroBytes = serializeAvro(record);

    PubsubMessage message = new PubsubMessage(avroBytes, Collections.emptyMap());

    PCollectionTuple results =
        pipeline
            .apply(Create.of(message))
            .apply(
                ParDo.of(new PubsubAvroToTableRow("perf_test_sub_a"))
                    .withOutputTags(
                        PubsubAvroToTableRow.SUCCESS_TAG,
                        TupleTagList.of(PubsubAvroToTableRow.DLQ_TAG)));

    PCollection<KV<String, TableRow>> success =
        results.get(PubsubAvroToTableRow.SUCCESS_TAG);

    PAssert.that(success)
        .satisfies(
            rows -> {
              KV<String, TableRow> kv = rows.iterator().next();
              String ts = (String) kv.getValue().get("timestamp");
              if (!expectedTimestamp.equals(ts)) {
                throw new AssertionError(
                    "Expected timestamp '"
                        + expectedTimestamp
                        + "', got '"
                        + ts
                        + "'");
              }
              return null;
            });

    pipeline.run();
  }
}

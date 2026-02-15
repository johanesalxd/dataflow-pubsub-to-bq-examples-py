package com.johanesalxd.transforms;

import com.google.api.services.bigquery.model.TableRow;
import java.io.ByteArrayInputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deserializes Avro binary messages from Pub/Sub and extracts typed fields.
 *
 * <p>Reads schemaless Avro binary (no container header), deserializes using the
 * embedded taxi_ride_v1 schema, and produces {@code KV<String, TableRow>} where
 * the key is the {@code ride_status} value (used for dynamic table routing) and
 * the value is a clean 14-field TableRow matching {@code TaxiRideBigQuerySchema}.
 *
 * <p>This mirrors the flatten-mapping pattern from the Python
 * {@code ParseKafkaMessage} DoFn, adapted for Avro binary input and
 * Pub/Sub metadata extraction.
 */
public class PubsubAvroToTableRow extends DoFn<PubsubMessage, KV<String, TableRow>> {

  private static final Logger LOG = LoggerFactory.getLogger(PubsubAvroToTableRow.class);

  public static final TupleTag<KV<String, TableRow>> SUCCESS_TAG =
      new TupleTag<KV<String, TableRow>>() {};
  public static final TupleTag<TableRow> DLQ_TAG = new TupleTag<TableRow>() {};

  /**
   * Avro schema JSON matching schemas/taxi_ride_v1.avsc.
   *
   * <p>Embedded as a constant so the schema is available on Dataflow workers
   * without needing to distribute .avsc files. The consumer knows the schema
   * a priori, matching the Kafka + Schema Registry pattern where schema is
   * cached on the consumer side.
   */
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
          + "{\"name\":\"timestamp\",\"type\":{\"type\":\"string\",\"logicalType\":\"iso-datetime\"}},"
          + "{\"name\":\"meter_reading\",\"type\":\"double\"},"
          + "{\"name\":\"meter_increment\",\"type\":\"double\"},"
          + "{\"name\":\"ride_status\",\"type\":\"string\"},"
          + "{\"name\":\"passenger_count\",\"type\":\"int\"}"
          + "]}";

  private final String subscriptionName;

  private transient Schema avroSchema;
  private transient GenericDatumReader<GenericRecord> datumReader;

  /**
   * Creates a new PubsubAvroToTableRow transform.
   *
   * @param subscriptionName short subscription name for metadata recording.
   */
  public PubsubAvroToTableRow(String subscriptionName) {
    this.subscriptionName = subscriptionName;
  }

  @Setup
  public void setup() {
    avroSchema = new Schema.Parser().parse(AVRO_SCHEMA_JSON);
    datumReader = new GenericDatumReader<>(avroSchema);
  }

  /**
   * Deserializes an Avro binary Pub/Sub message into a routable TableRow.
   *
   * @param message the Pub/Sub message containing Avro binary payload.
   * @param timestamp the Pub/Sub publish timestamp.
   * @param out multi-output receiver for success and DLQ paths.
   */
  @ProcessElement
  public void processElement(
      @Element PubsubMessage message,
      @Timestamp Instant timestamp,
      MultiOutputReceiver out) {

    byte[] payload = message.getPayload();

    try {
      // Deserialize Avro binary (schemaless -- no container header)
      BinaryDecoder decoder =
          DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(payload), null);
      GenericRecord record = datumReader.read(null, decoder);

      // Extract typed fields from Avro GenericRecord
      String rideId = record.get("ride_id").toString();
      int pointIdx = (int) record.get("point_idx");
      double latitude = (double) record.get("latitude");
      double longitude = (double) record.get("longitude");
      String rideTimestamp = record.get("timestamp").toString();
      double meterReading = (double) record.get("meter_reading");
      double meterIncrement = (double) record.get("meter_increment");
      String rideStatus = record.get("ride_status").toString();
      int passengerCount = (int) record.get("passenger_count");

      // Extract Pub/Sub metadata
      String messageId = message.getMessageId();
      Map<String, String> attributes = message.getAttributeMap();
      String attributesJson =
          attributes != null ? new JSONObject(attributes).toString() : "{}";

      // Build typed TableRow (14 fields, matching TaxiRideBigQuerySchema)
      TableRow row = new TableRow();
      // Envelope metadata
      row.set("subscription_name", this.subscriptionName);
      row.set("message_id", messageId);
      row.set("publish_time", timestamp.toString());
      row.set("processing_time", Instant.now().toString());
      // Taxi ride data
      row.set("ride_id", rideId);
      row.set("point_idx", pointIdx);
      row.set("latitude", latitude);
      row.set("longitude", longitude);
      row.set("timestamp", rideTimestamp);
      row.set("meter_reading", meterReading);
      row.set("meter_increment", meterIncrement);
      row.set("ride_status", rideStatus);
      row.set("passenger_count", passengerCount);
      // Pub/Sub attributes
      row.set("attributes", attributesJson);

      // Output KV: key = ride_status (routing), value = clean TableRow (data)
      out.get(SUCCESS_TAG).output(KV.of(rideStatus, row));

    } catch (Exception e) {
      String errorMessage =
          e.getMessage() != null ? e.getMessage() : e.getClass().getName();
      LOG.error("Error processing Avro message: {}", errorMessage);

      // Extract stack trace
      StringWriter sw = new StringWriter();
      e.printStackTrace(new PrintWriter(sw));

      TableRow errorRow = new TableRow();
      errorRow.set("processing_time", Instant.now().toString());
      errorRow.set("error_message", errorMessage);
      errorRow.set("stack_trace", sw.toString());
      errorRow.set(
          "original_payload",
          payload != null
              ? new String(payload, StandardCharsets.UTF_8)
              : "");
      errorRow.set("subscription_name", this.subscriptionName);

      out.get(DLQ_TAG).output(errorRow);
    }
  }
}

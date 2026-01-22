package com.johanesalxd.transforms;

import com.google.api.services.bigquery.model.TableRow;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transform to parse Pub/Sub messages and convert to BigQuery TableRow with Raw JSON.
 */
public class PubsubMessageToRawJson extends DoFn<PubsubMessage, TableRow> {

  private static final Logger LOG = LoggerFactory.getLogger(PubsubMessageToRawJson.class);
  
  public static final TupleTag<TableRow> SUCCESS_TAG = new TupleTag<TableRow>() {};
  public static final TupleTag<TableRow> DLQ_TAG = new TupleTag<TableRow>() {};

  private final String subscriptionName;

  public PubsubMessageToRawJson(String subscriptionName) {
    this.subscriptionName = subscriptionName;
  }

  @ProcessElement
  public void processElement(@Element PubsubMessage message, @Timestamp Instant timestamp, MultiOutputReceiver out) {
    String payload = "";
    
    try {
      // Decode the message data
      payload = new String(message.getPayload(), StandardCharsets.UTF_8);

      // Verify it's valid JSON (just to be safe, like the Python version)
      // If it fails, it will go to the catch block
      new JSONObject(payload);

      // Extract Pub/Sub metadata
      String messageId = message.getMessageId();
      
      // Extract custom attributes
      Map<String, String> attributes = message.getAttributeMap();
      String attributesJson = attributes != null ? new JSONObject(attributes).toString() : "{}";

      TableRow row = new TableRow();
      row.set("subscription_name", this.subscriptionName);
      row.set("message_id", messageId);
      row.set("publish_time", timestamp.toString());
      row.set("processing_time", Instant.now().toString());
      row.set("attributes", attributesJson);
      // In Python: json.dumps(json_payload). In Java, payload is already the JSON string.
      // BQ JSON type expects a JSON string.
      row.set("payload", payload);

      out.get(SUCCESS_TAG).output(row);

    } catch (Exception e) {
      LOG.error("Error processing message: " + e.getMessage());
      
      // Extract stack trace
      StringWriter sw = new StringWriter();
      e.printStackTrace(new PrintWriter(sw));
      
      TableRow errorRow = new TableRow();
      errorRow.set("processing_time", Instant.now().toString());
      errorRow.set("error_message", e.getMessage());
      errorRow.set("stack_trace", sw.toString());
      errorRow.set("original_payload", payload.isEmpty() && message.getPayload() != null 
          ? new String(message.getPayload(), StandardCharsets.UTF_8) 
          : payload);
      errorRow.set("subscription_name", this.subscriptionName);
      
      out.get(DLQ_TAG).output(errorRow);
    }
  }
}

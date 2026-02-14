package com.johanesalxd.schemas;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import java.util.Arrays;

public class RawJsonBigQuerySchema {
  /**
   * Returns the BigQuery schema for the taxi_events_json table.
   */
  public static TableSchema getSchema() {
    return new TableSchema().setFields(Arrays.asList(
        new TableFieldSchema()
            .setName("subscription_name")
            .setType("STRING")
            .setMode("NULLABLE"),
        new TableFieldSchema()
            .setName("message_id")
            .setType("STRING")
            .setMode("NULLABLE"),
        new TableFieldSchema()
            .setName("publish_time")
            .setType("TIMESTAMP")
            .setMode("NULLABLE"),
        new TableFieldSchema()
            .setName("processing_time")
            .setType("TIMESTAMP")
            .setMode("NULLABLE"),
        new TableFieldSchema()
            .setName("attributes")
            .setType("STRING")
            .setMode("NULLABLE"),
        new TableFieldSchema()
            .setName("payload")
            .setType("JSON") // Using JSON type directly
            .setMode("NULLABLE")
    ));
  }
}

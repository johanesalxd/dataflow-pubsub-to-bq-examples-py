package com.johanesalxd.schemas;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import java.util.Arrays;

public class DeadLetterBigQuerySchema {
  /**
   * Returns the BigQuery schema for the dead letter queue table.
   */
  public static TableSchema getSchema() {
    return new TableSchema().setFields(Arrays.asList(
        new TableFieldSchema()
            .setName("processing_time")
            .setType("TIMESTAMP")
            .setMode("NULLABLE"),
        new TableFieldSchema()
            .setName("error_message")
            .setType("STRING")
            .setMode("NULLABLE"),
        new TableFieldSchema()
            .setName("stack_trace")
            .setType("STRING")
            .setMode("NULLABLE"),
        new TableFieldSchema()
            .setName("original_payload")
            .setType("STRING")
            .setMode("NULLABLE"),
        new TableFieldSchema()
            .setName("subscription_name")
            .setType("STRING")
            .setMode("NULLABLE")
    ));
  }
}

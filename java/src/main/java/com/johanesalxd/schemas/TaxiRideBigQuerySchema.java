package com.johanesalxd.schemas;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import java.util.Arrays;

/**
 * BigQuery schema for typed taxi ride tables.
 *
 * <p>Defines the 14-column flattened schema matching the Python
 * {@code get_bigquery_schema()} in {@code json_to_tablerow.py}. This schema
 * is used by the Avro consumer pipeline for dynamic destination tables
 * (one per ride_status value).
 */
public class TaxiRideBigQuerySchema {

  /**
   * Returns the BigQuery schema for typed taxi ride tables.
   *
   * <p>Contains 14 fields: 4 envelope metadata fields, 9 taxi ride data
   * fields, and 1 attributes field.
   *
   * @return TableSchema with all field definitions.
   */
  public static TableSchema getSchema() {
    return new TableSchema()
        .setFields(
            Arrays.asList(
                // Envelope metadata
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
                // Taxi ride data
                new TableFieldSchema()
                    .setName("ride_id")
                    .setType("STRING")
                    .setMode("NULLABLE"),
                new TableFieldSchema()
                    .setName("point_idx")
                    .setType("INT64")
                    .setMode("NULLABLE"),
                new TableFieldSchema()
                    .setName("latitude")
                    .setType("FLOAT64")
                    .setMode("NULLABLE"),
                new TableFieldSchema()
                    .setName("longitude")
                    .setType("FLOAT64")
                    .setMode("NULLABLE"),
                new TableFieldSchema()
                    .setName("timestamp")
                    .setType("TIMESTAMP")
                    .setMode("NULLABLE"),
                new TableFieldSchema()
                    .setName("meter_reading")
                    .setType("FLOAT64")
                    .setMode("NULLABLE"),
                new TableFieldSchema()
                    .setName("meter_increment")
                    .setType("FLOAT64")
                    .setMode("NULLABLE"),
                new TableFieldSchema()
                    .setName("ride_status")
                    .setType("STRING")
                    .setMode("NULLABLE"),
                new TableFieldSchema()
                    .setName("passenger_count")
                    .setType("INT64")
                    .setMode("NULLABLE"),
                // Pub/Sub attributes
                new TableFieldSchema()
                    .setName("attributes")
                    .setType("STRING")
                    .setMode("NULLABLE")));
  }
}

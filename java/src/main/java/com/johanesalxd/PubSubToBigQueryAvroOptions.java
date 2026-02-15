package com.johanesalxd;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/**
 * Pipeline options for the Avro consumer with dynamic table routing.
 */
public interface PubSubToBigQueryAvroOptions extends PipelineOptions {

  @Description(
      "Pub/Sub subscription path "
          + "(e.g., projects/PROJECT_ID/subscriptions/SUBSCRIPTION_NAME)")
  @Validation.Required
  String getSubscription();

  void setSubscription(String value);

  @Description(
      "BigQuery output table base reference (e.g., PROJECT_ID:DATASET.TABLE_PREFIX). "
          + "Routes to TABLE_PREFIX_enroute, TABLE_PREFIX_pickup, etc. "
          + "DLQ writes to TABLE_PREFIX_dlq.")
  @Validation.Required
  String getOutputTableBase();

  void setOutputTableBase(String value);

  @Description("Subscription name to record in BigQuery metadata")
  @Validation.Required
  String getSubscriptionName();

  void setSubscriptionName(String value);
}

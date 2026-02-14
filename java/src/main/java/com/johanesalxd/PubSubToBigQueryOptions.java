package com.johanesalxd;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface PubSubToBigQueryOptions extends PipelineOptions {
  @Description("Pub/Sub subscription name (e.g., projects/PROJECT_ID/subscriptions/SUBSCRIPTION_NAME)")
  @Validation.Required
  String getSubscription();

  void setSubscription(String value);

  @Description("BigQuery output table (e.g., PROJECT_ID:DATASET.TABLE)")
  @Validation.Required
  String getOutputTable();

  void setOutputTable(String value);

  @Description("Subscription name to record in BigQuery metadata")
  @Validation.Required
  String getSubscriptionName();

  void setSubscriptionName(String value);
}

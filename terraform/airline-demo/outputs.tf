output "source_topics" {
  value       = ["flight_updates", "passenger_itineraries", "rebooking_inventory", "gate_crew_status"]
  description = "Source topics datagen produces into"
}

output "served_topics" {
  value       = ["passenger_journey", "passenger_recovery", "flight_ops_state"]
  description = "Maintained/served tables (Lightning + RTCE read these)"
}

output "keynote_source_topics" {
  value       = ["flight_status", "passenger_connections", "hotel_inventory"]
  description = "Keynote source feeds"
}

output "keynote_served_topics" {
  value       = ["passenger_risk", "flight_impact", "passenger_recommendations"]
  description = "Keynote Flink outputs and recovery offers"
}

output "streaming_agent_enabled" {
  value       = local.agent_enabled
  description = "Whether the recovery model/agent/INSERT were deployed (needs Bedrock creds in core)"
}

output "tableflow_topics" {
  value       = var.enable_tableflow ? var.tableflow_topics : []
  description = "Topics exposed as Iceberg tables via Tableflow (Confluent Managed Storage) — the open history/analytics path (pillar 7)"
}

output "keynote_analytics_bucket" {
  value       = local.keynote_analytics_enabled ? aws_s3_bucket.keynote_analytics[0].bucket : ""
  description = "S3 bucket holding the keynote topics' Iceberg data/metadata (Demo 3)"
}

output "keynote_tableflow_role_name" {
  value       = local.keynote_analytics_enabled ? aws_iam_role.keynote_tableflow[0].name : ""
  description = "AWS IAM role Confluent Tableflow assumes for the keynote topics. If Glue sync errors with AccessDenied, regenerate the Glue permission policy at Confluent Cloud Console > Environment > Tableflow > Catalog Integration > AWS Glue > Configure AWS Glue access, and reconcile it with aws_iam_policy.keynote_tableflow_glue."
}

output "keynote_glue_database" {
  value       = local.keynote_analytics_enabled ? local.keynote_glue_database : ""
  description = "AWS Glue database name the keynote Iceberg tables sync into (defaults to the Kafka cluster ID)"
}

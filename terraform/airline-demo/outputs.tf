output "source_topics" {
  value       = ["flight_updates", "passenger_itineraries", "rebooking_inventory", "gate_crew_status"]
  description = "Source topics datagen produces into"
}

output "served_topics" {
  value       = ["passenger_journey", "passenger_recovery", "flight_ops_state"]
  description = "Maintained/served tables (Lightning + RTCE read these)"
}

output "streaming_agent_enabled" {
  value       = local.agent_enabled
  description = "Whether the recovery model/agent/INSERT were deployed (needs Bedrock creds in core)"
}

output "tableflow_topics" {
  value       = var.enable_tableflow ? var.tableflow_topics : []
  description = "Topics exposed as Iceberg tables via Tableflow (Confluent Managed Storage) — the open history/analytics path (pillar 7)"
}

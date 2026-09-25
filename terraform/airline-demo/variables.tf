# airline-demo takes no credentials of its own — everything is inherited from core
# via terraform_remote_state. The only knob is whether to build the recovery
# streaming agent, which additionally requires core to have a Bedrock connection.

variable "enable_streaming_agent" {
  description = "Build the recovery model + agent + recovery INSERT. Also requires Bedrock creds in core (auto-skipped if core has no Bedrock connection)."
  type        = bool
  default     = true
}

variable "enable_tableflow" {
  description = "Enable Tableflow (Confluent Managed Storage -> Iceberg) on the served tables so history + outcomes are open for analytics (demo pillar 7). Managed storage needs no S3 bucket or provider integration."
  type        = bool
  default     = true
}

variable "tableflow_topics" {
  description = "Topics to expose as Iceberg tables via Tableflow. Default: the maintained entity, the recovery outcomes, and the ops rollup."
  type        = list(string)
  default     = ["passenger_journey", "passenger_recovery", "flight_ops_state"]
}

variable "enable_keynote_analytics" {
  description = "Enable Tableflow with customer-owned S3 storage + an AWS Glue Data Catalog integration on the keynote topics, for the Demo 3 Athena/Amazon Quick analytics scene. This toggle alone isn't enough — it also needs core's aws_tableflow_access_key/secret_key set (optional, like aws_bedrock_access_key; auto-skipped when empty, see local.keynote_analytics_enabled). Unlike enable_tableflow's Confluent Managed Storage, this creates a real S3 bucket and a cross-account IAM role with S3/IAM/Glue permissions."
  type        = bool
  default     = true
}

variable "keynote_tableflow_topics" {
  description = "Keynote topics to expose as Iceberg tables in the customer-owned S3 bucket, synced to AWS Glue. Default matches the keynote data contract's Athena question: flight status, passenger risk, and the recovery offers."
  type        = list(string)
  default     = ["flight_status", "passenger_risk", "passenger_recommendations"]
}

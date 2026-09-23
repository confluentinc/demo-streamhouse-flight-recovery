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

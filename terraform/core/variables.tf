# ---------------------------------------------------------------------------
# Core infrastructure variables — AWS-only.
#
# This demo is AWS-only by design: Real-Time Context Engine (RTCE) and Flink
# Native Inference / Bedrock are both AWS-only. There is deliberately no Azure
# path here.
# ---------------------------------------------------------------------------

variable "resource_prefix" {
  description = "Prefix applied to every named Confluent resource (env, cluster, compute pool, service accounts). A random suffix is appended so multiple deployments can coexist in one org."
  type        = string
  default     = "flight-recovery"

  validation {
    condition     = can(regex("^[a-z0-9-]{1,32}$", var.resource_prefix))
    error_message = "resource_prefix must be lowercase alphanumeric/hyphen, 1-32 chars."
  }
}

variable "cloud_region" {
  description = "AWS region for deployment. Must be an RTCE-supported region (default us-east-1)."
  type        = string
  default     = "us-east-1"
}

variable "confluent_cloud_api_key" {
  description = "Confluent Cloud API Key"
  type        = string
  sensitive   = true
}

variable "confluent_cloud_api_secret" {
  description = "Confluent Cloud API Secret"
  type        = string
  sensitive   = true
}

# AWS Bedrock credentials — used to create the managed Flink Bedrock connection
# that powers the recovery streaming agent (built in terraform/airline-demo).
variable "aws_bedrock_access_key" {
  description = "AWS Access Key ID for Bedrock"
  type        = string
  sensitive   = true
  default     = ""
}

variable "aws_bedrock_secret_key" {
  description = "AWS Secret Access Key for Bedrock"
  type        = string
  sensitive   = true
  default     = ""
}

variable "aws_session_token" {
  description = "AWS Session Token for temporary credentials (required when the access key starts with ASIA)"
  type        = string
  sensitive   = true
  default     = ""
}

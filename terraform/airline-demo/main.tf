# ===========================================================================
# airline-demo — the airport-disruption-recovery Flink pipeline.
#
# Every Flink object is one `confluent_flink_statement` whose body is loaded
# verbatim from a .sql file in ./sql (the single source of truth the Walkthrough
# also references). Bare table/model names resolve against the catalog + database
# set in each statement's `properties`, so the .sql files stay paste-ready.
#
# Ordering:
#   source + recovery-sink tables (for_each, parallel)
#     -> passenger_journey MATERIALIZED TABLE -> flight_ops_state MATERIALIZED TABLE
#   recovery model -> recovery agent -> recovery INSERT   (gated on Bedrock)
#
# The two derived serving tables (passenger_journey, flight_ops_state) are
# MATERIALIZED TABLES: one object owning both the table and its continuous query,
# evolvable in place with CREATE OR ALTER (Flink migrates to the same backing topic,
# so Lightning/RTCE/Tableflow are unaffected). passenger_recovery stays a plain
# CREATE TABLE + INSERT INTO because its query calls AI_RUN_AGENT, which is not
# supported inside a materialized table (FSE-1525 / MATRIX-1740).
# ===========================================================================

data "terraform_remote_state" "core" {
  backend = "local"
  config = {
    path = "../core/terraform.tfstate"
  }
}

data "confluent_organization" "main" {}

data "confluent_flink_region" "airport" {
  cloud  = "AWS"
  region = data.terraform_remote_state.core.outputs.cloud_region
}

locals {
  core          = data.terraform_remote_state.core.outputs
  catalog       = local.core.confluent_environment_display_name
  database      = local.core.confluent_kafka_cluster_display_name
  rest_endpoint = data.confluent_flink_region.airport.rest_endpoint

  sql_props = {
    "sql.current-catalog"  = local.catalog
    "sql.current-database" = local.database
  }

  # The recovery agent needs both the toggle AND a Bedrock connection in core.
  agent_enabled = var.enable_streaming_agent && local.core.bedrock_enabled

  # Source tables (datagen inputs) + the passenger_recovery sink — no
  # interdependencies, created in parallel. The derived passenger_journey and
  # flight_ops_state tables are materialized tables, defined as their own resources
  # below. passenger_recovery stays a plain CREATE TABLE (its INSERT calls
  # AI_RUN_AGENT, unsupported in a materialized table).
  tables = {
    "flight_updates"        = "sql/01-source-flight-updates.sql"
    "passenger_itineraries" = "sql/02-source-passenger-itineraries.sql"
    "rebooking_inventory"   = "sql/03-source-rebooking-inventory.sql"
    "gate_crew_status"      = "sql/04-source-gate-crew-status.sql"
    "passenger_recovery"    = "sql/06-serving-passenger-recovery.sql"
  }
}

# --- Tables (source + serving) --------------------------------------------
resource "confluent_flink_statement" "tables" {
  for_each = local.tables

  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = local.core.confluent_environment_id
  }
  compute_pool {
    id = local.core.confluent_flink_compute_pool_id
  }
  principal {
    id = local.core.app_manager_service_account_id
  }
  rest_endpoint = local.rest_endpoint
  credentials {
    key    = local.core.app_manager_flink_api_key
    secret = local.core.app_manager_flink_api_secret
  }

  statement_name = "airport-create-${replace(each.key, "_", "-")}"
  statement      = file("${path.module}/${each.value}")
  properties     = local.sql_props

  lifecycle {
    prevent_destroy = false
  }
}

# --- The central governed table: passenger_journey (MATERIALIZED TABLE) -----
# One object owning the table + its continuous query; CREATE OR ALTER evolves it
# in place. A .sql change replaces this statement, which re-runs CREATE OR ALTER
# and migrates the query onto the same backing topic (consumers unaffected).
resource "confluent_flink_statement" "materialized_passenger_journey" {
  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = local.core.confluent_environment_id
  }
  compute_pool {
    id = local.core.confluent_flink_compute_pool_id
  }
  principal {
    id = local.core.app_manager_service_account_id
  }
  rest_endpoint = local.rest_endpoint
  credentials {
    key    = local.core.app_manager_flink_api_key
    secret = local.core.app_manager_flink_api_secret
  }

  statement_name = "airport-materialize-passenger-journey"
  statement      = file("${path.module}/sql/05-serving-passenger-journey.sql")
  properties     = local.sql_props

  depends_on = [confluent_flink_statement.tables]
}

# --- Ops spoke: flight_ops_state (MATERIALIZED TABLE) ----------------------
# Aggregates passenger_journey, so it depends on that materialized table existing.
resource "confluent_flink_statement" "materialized_flight_ops_state" {
  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = local.core.confluent_environment_id
  }
  compute_pool {
    id = local.core.confluent_flink_compute_pool_id
  }
  principal {
    id = local.core.app_manager_service_account_id
  }
  rest_endpoint = local.rest_endpoint
  credentials {
    key    = local.core.app_manager_flink_api_key
    secret = local.core.app_manager_flink_api_secret
  }

  statement_name = "airport-materialize-flight-ops-state"
  statement      = file("${path.module}/sql/07-serving-flight-ops-state.sql")
  properties     = local.sql_props

  depends_on = [confluent_flink_statement.materialized_passenger_journey]
}

# --- Recovery model (gated on Bedrock) ------------------------------------
resource "confluent_flink_statement" "recovery_model" {
  count = local.agent_enabled ? 1 : 0

  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = local.core.confluent_environment_id
  }
  compute_pool {
    id = local.core.confluent_flink_compute_pool_id
  }
  principal {
    id = local.core.app_manager_service_account_id
  }
  rest_endpoint = local.rest_endpoint
  credentials {
    key    = local.core.app_manager_flink_api_key
    secret = local.core.app_manager_flink_api_secret
  }

  statement_name = "airport-create-recovery-model"
  statement      = file("${path.module}/sql/10-model-recovery.sql")
  properties     = local.sql_props

  lifecycle {
    ignore_changes = [statement]
  }
}

# --- Recovery agent (gated on Bedrock) ------------------------------------
resource "confluent_flink_statement" "recovery_agent" {
  count = local.agent_enabled ? 1 : 0

  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = local.core.confluent_environment_id
  }
  compute_pool {
    id = local.core.confluent_flink_compute_pool_id
  }
  principal {
    id = local.core.app_manager_service_account_id
  }
  rest_endpoint = local.rest_endpoint
  credentials {
    key    = local.core.app_manager_flink_api_key
    secret = local.core.app_manager_flink_api_secret
  }

  statement_name = "airport-create-recovery-agent"
  statement      = file("${path.module}/sql/11-agent-recovery.sql")
  properties     = local.sql_props

  lifecycle {
    ignore_changes = [statement]
  }

  depends_on = [confluent_flink_statement.recovery_model]
}

# --- Concierge spoke: passenger_recovery (gated on Bedrock) ----------------
resource "confluent_flink_statement" "insert_passenger_recovery" {
  count = local.agent_enabled ? 1 : 0

  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = local.core.confluent_environment_id
  }
  compute_pool {
    id = local.core.confluent_flink_compute_pool_id
  }
  principal {
    id = local.core.app_manager_service_account_id
  }
  rest_endpoint = local.rest_endpoint
  credentials {
    key    = local.core.app_manager_flink_api_key
    secret = local.core.app_manager_flink_api_secret
  }

  statement_name = "airport-insert-passenger-recovery"
  statement      = file("${path.module}/sql/12-insert-passenger-recovery.sql")
  properties     = local.sql_props

  depends_on = [
    confluent_flink_statement.tables,
    confluent_flink_statement.materialized_passenger_journey,
    confluent_flink_statement.recovery_agent,
  ]
}

# --- Tableflow (pillar 7): open history + outcomes as Iceberg ----------------
# Enable Tableflow on the served tables so their history is materialized to
# Apache Iceberg for open, analytical/lakehouse use. Confluent Managed Storage
# (managed_storage {}) means no S3 bucket, provider integration, or IAM trust
# policy to wire up. Enablement requires a Tableflow-scoped API key owned by a
# principal that can enable Tableflow — app-manager holds EnvironmentAdmin.
# (Our RTCE Global key can't: it belongs to the DeveloperRead-only rtce-reader SA.)
resource "confluent_api_key" "tableflow" {
  count = var.enable_tableflow ? 1 : 0

  display_name = "${local.core.resource_prefix}-${local.core.random_id}-tableflow-api-key"
  description  = "Tableflow API key owned by the app-manager service account"
  owner {
    id          = local.core.app_manager_service_account_id
    api_version = "iam/v2"
    kind        = "ServiceAccount"
  }

  managed_resource {
    id          = "tableflow"
    api_version = "tableflow/v1"
    kind        = "Tableflow"
  }
}

resource "confluent_tableflow_topic" "history" {
  for_each = var.enable_tableflow ? toset(var.tableflow_topics) : toset([])

  environment {
    id = local.core.confluent_environment_id
  }
  kafka_cluster {
    id = local.core.confluent_kafka_cluster_id
  }
  display_name  = each.value
  table_formats = ["ICEBERG"]
  managed_storage {}
  credentials {
    key    = confluent_api_key.tableflow[0].id
    secret = confluent_api_key.tableflow[0].secret
  }

  # The topic must exist first. Source/recovery topics come from the CREATE TABLE
  # statements; passenger_journey and flight_ops_state topics come from their
  # materialized-table statements.
  depends_on = [
    confluent_flink_statement.tables,
    confluent_flink_statement.materialized_passenger_journey,
    confluent_flink_statement.materialized_flight_ops_state,
  ]
}

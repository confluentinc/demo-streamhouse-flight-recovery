# ===========================================================================
# airline-demo — the River Air flight-recovery Flink pipeline.
#
# Every Flink object is one `confluent_flink_statement` whose body is loaded
# verbatim from a .sql file in ./sql (the single source of truth the Walkthrough
# also references). Bare table names resolve against the catalog + database set
# in each statement's `properties`, so the .sql files stay paste-ready.
#
# Ordering:
#   flight_status, passenger_itineraries, hotel_inventory source tables
#   + the passenger_recommendations table   (for_each, parallel)
#     -> passenger_state MATERIALIZED TABLE -> flight_impact MATERIALIZED TABLE
#   recovery agent (gated, see local.agent_enabled):
#     passenger_state -> passenger_state_changes -> impacted_passengers (append) ─┐
#     model -> live_context tool -> agent ─────────────────────────────────────────┴-> INSERT INTO passenger_recommendations
#
# The two derived serving tables (passenger_state, flight_impact) are
# MATERIALIZED TABLES: one object owning both the table and its continuous query,
# evolvable in place with CREATE OR ALTER (Flink migrates to the same backing topic,
# so Lightning/RTCE/Tableflow are unaffected). Tableflow syncs selected tables to
# Iceberg in S3 with an AWS Glue Data Catalog for Demo 3 — see analytics.tf.
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

  # Demo 3's S3/Glue path needs both the toggle AND AWS Tableflow creds in core.
  # Leave the creds empty and this cleanly skips instead of failing terraform
  # apply. aws_tableflow_credentials_present is a boolean, not a secret, but
  # Terraform marks it sensitive anyway since it's derived from
  # var.aws_tableflow_access_key — nonsensitive() is needed because
  # count/for_each can't take a sensitive value. try() treats a core state from
  # before this output existed as "no creds", so plan and destroy still work.
  analytics_enabled = var.enable_analytics && try(nonsensitive(local.core.aws_tableflow_credentials_present), false)

  # The recovery agent needs Claude on Bedrock (core) and the RTCE connection, which
  # `uv run deploy` creates with the CLI after RTCE is enabled and then sets the toggle.
  agent_enabled = var.enable_recovery_agent && local.core.bedrock_enabled

  agent_setup = {
    passenger_state_changes = "sql/26-staging-passenger-state-changes.sql"
    model                   = "sql/28-model-passenger-recovery.sql"
    tool                    = "sql/29-tool-live-context.sql"
  }
}

# --- Tables (three source feeds + recovery offers) --------------------------
# No interdependencies, created in parallel. The datagen publishes into the
# three sources and writes passenger_recommendations rows directly. Statement
# names use "airport-table-*" so they don't collide with the "airport-create-*"
# statements an older deployment may still have running.
resource "confluent_flink_statement" "tables" {
  for_each = {
    flight_status             = "sql/20-source-flight-status.sql"
    passenger_itineraries     = "sql/21-source-passenger-itineraries.sql"
    hotel_inventory           = "sql/22-source-hotel-inventory.sql"
    passenger_recommendations = "sql/23-serving-passenger-recommendations.sql"
  }

  organization { id = data.confluent_organization.main.id }
  environment { id = local.core.confluent_environment_id }
  compute_pool { id = local.core.confluent_flink_compute_pool_id }
  principal { id = local.core.app_manager_service_account_id }
  rest_endpoint = local.rest_endpoint
  credentials {
    key    = local.core.app_manager_flink_api_key
    secret = local.core.app_manager_flink_api_secret
  }

  statement_name = "airport-table-${replace(each.key, "_", "-")}"
  statement      = file("${path.module}/${each.value}")
  properties     = local.sql_props
}

# --- Per-passenger connection risk: passenger_state (MATERIALIZED TABLE) ----
# Joins passenger_itineraries to flight_status. A .sql change replaces this
# statement, which re-runs CREATE OR ALTER and migrates the query onto the same
# backing topic (consumers unaffected).
resource "confluent_flink_statement" "passenger_state" {
  organization { id = data.confluent_organization.main.id }
  environment { id = local.core.confluent_environment_id }
  compute_pool { id = local.core.confluent_flink_compute_pool_id }
  principal { id = local.core.app_manager_service_account_id }
  rest_endpoint = local.rest_endpoint
  credentials {
    key    = local.core.app_manager_flink_api_key
    secret = local.core.app_manager_flink_api_secret
  }

  statement_name = "airport-materialize-passenger-state"
  statement      = file("${path.module}/sql/24-serving-passenger-state.sql")
  properties     = local.sql_props

  depends_on = [confluent_flink_statement.tables]
}

# --- Per-flight rollup: flight_impact (MATERIALIZED TABLE) ------------------
# Counts HIGH-risk passengers from passenger_state, so it depends on that
# materialized table existing.
resource "confluent_flink_statement" "flight_impact" {
  organization { id = data.confluent_organization.main.id }
  environment { id = local.core.confluent_environment_id }
  compute_pool { id = local.core.confluent_flink_compute_pool_id }
  principal { id = local.core.app_manager_service_account_id }
  rest_endpoint = local.rest_endpoint
  credentials {
    key    = local.core.app_manager_flink_api_key
    secret = local.core.app_manager_flink_api_secret
  }

  statement_name = "airport-materialize-flight-impact"
  statement      = file("${path.module}/sql/25-serving-flight-impact.sql")
  properties     = local.sql_props

  depends_on = [confluent_flink_statement.passenger_state]
}

# --- Recovery agent (gated on Bedrock + the RTCE connection) ----------------
# The append-only change table, the model, and the RTCE tool have no
# interdependencies. impacted_passengers reads the change table. The agent needs
# the model and tool; the INSERT needs the agent, impacted_passengers, and
# passenger_recommendations.
resource "confluent_flink_statement" "agent_setup" {
  for_each = local.agent_enabled ? local.agent_setup : {}

  organization { id = data.confluent_organization.main.id }
  environment { id = local.core.confluent_environment_id }
  compute_pool { id = local.core.confluent_flink_compute_pool_id }
  principal { id = local.core.app_manager_service_account_id }
  rest_endpoint = local.rest_endpoint
  credentials {
    key    = local.core.app_manager_flink_api_key
    secret = local.core.app_manager_flink_api_secret
  }

  statement_name = "airport-agent-${replace(each.key, "_", "-")}"
  statement      = file("${path.module}/${each.value}")
  properties     = local.sql_props

  depends_on = [confluent_flink_statement.passenger_state]
}

resource "confluent_flink_statement" "impacted_passengers" {
  count = local.agent_enabled ? 1 : 0

  organization { id = data.confluent_organization.main.id }
  environment { id = local.core.confluent_environment_id }
  compute_pool { id = local.core.confluent_flink_compute_pool_id }
  principal { id = local.core.app_manager_service_account_id }
  rest_endpoint = local.rest_endpoint
  credentials {
    key    = local.core.app_manager_flink_api_key
    secret = local.core.app_manager_flink_api_secret
  }

  statement_name = "airport-agent-impacted-passengers"
  statement      = file("${path.module}/sql/27-staging-impacted-passengers.sql")
  properties     = local.sql_props

  depends_on = [confluent_flink_statement.agent_setup]
}

resource "confluent_flink_statement" "recovery_agent" {
  count = local.agent_enabled ? 1 : 0

  organization { id = data.confluent_organization.main.id }
  environment { id = local.core.confluent_environment_id }
  compute_pool { id = local.core.confluent_flink_compute_pool_id }
  principal { id = local.core.app_manager_service_account_id }
  rest_endpoint = local.rest_endpoint
  credentials {
    key    = local.core.app_manager_flink_api_key
    secret = local.core.app_manager_flink_api_secret
  }

  statement_name = "airport-agent-passenger-recovery"
  statement      = file("${path.module}/sql/30-agent-passenger-recovery.sql")
  properties     = local.sql_props

  depends_on = [confluent_flink_statement.agent_setup]
}

resource "confluent_flink_statement" "recovery_offers" {
  count = local.agent_enabled ? 1 : 0

  organization { id = data.confluent_organization.main.id }
  environment { id = local.core.confluent_environment_id }
  compute_pool { id = local.core.confluent_flink_compute_pool_id }
  principal { id = local.core.app_manager_service_account_id }
  rest_endpoint = local.rest_endpoint
  credentials {
    key    = local.core.app_manager_flink_api_key
    secret = local.core.app_manager_flink_api_secret
  }

  statement_name = "airport-agent-insert-passenger-recommendations"
  statement      = file("${path.module}/sql/31-insert-passenger-recommendations.sql")
  properties     = local.sql_props

  depends_on = [
    confluent_flink_statement.tables,
    confluent_flink_statement.impacted_passengers,
    confluent_flink_statement.recovery_agent,
  ]
}

# --- Tableflow API key ------------------------------------------------------
# Enabling Tableflow (analytics.tf) requires a Tableflow-scoped API key owned by
# a principal that can enable Tableflow — app-manager holds EnvironmentAdmin.
# (Our RTCE Global key can't: it belongs to the DeveloperRead-only rtce-reader SA.)
resource "confluent_api_key" "tableflow" {
  count = local.analytics_enabled ? 1 : 0

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

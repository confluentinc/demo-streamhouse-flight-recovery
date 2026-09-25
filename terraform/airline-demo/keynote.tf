# The keynote pipeline is additive while the earlier demo is still deployed.
# The legacy passenger_itineraries topic has seven fields, so this deployment
# uses passenger_connections for the three-field source contract.
locals {
  keynote_source_sql = {
    flight_status             = "sql/20-source-flight-status.sql"
    passenger_connections     = "sql/21-source-passenger-connections.sql"
    hotel_inventory           = "sql/22-source-hotel-inventory.sql"
    passenger_recommendations = "sql/23-serving-passenger-recommendations.sql"
  }
}

resource "confluent_flink_statement" "keynote_tables" {
  for_each = local.keynote_source_sql

  organization { id = data.confluent_organization.main.id }
  environment { id = local.core.confluent_environment_id }
  compute_pool { id = local.core.confluent_flink_compute_pool_id }
  principal { id = local.core.app_manager_service_account_id }
  rest_endpoint = local.rest_endpoint
  credentials {
    key    = local.core.app_manager_flink_api_key
    secret = local.core.app_manager_flink_api_secret
  }

  statement_name = "airport-keynote-create-${replace(each.key, "_", "-")}"
  statement      = file("${path.module}/${each.value}")
  properties     = local.sql_props
}

resource "confluent_flink_statement" "keynote_passenger_risk" {
  organization { id = data.confluent_organization.main.id }
  environment { id = local.core.confluent_environment_id }
  compute_pool { id = local.core.confluent_flink_compute_pool_id }
  principal { id = local.core.app_manager_service_account_id }
  rest_endpoint = local.rest_endpoint
  credentials {
    key    = local.core.app_manager_flink_api_key
    secret = local.core.app_manager_flink_api_secret
  }

  # This statement first created the live materialized table through Confluent MCP.
  # Import it into Terraform rather than re-running CREATE OR ALTER, which Flink
  # rejects on an already materialized table as a distribution change.
  statement_name = "keynote-materialize-passenger-risk-20260924"
  statement      = file("${path.module}/sql/24-serving-passenger-risk.sql")
  properties     = local.sql_props

  lifecycle { ignore_changes = [statement] }

  depends_on = [confluent_flink_statement.keynote_tables]
}

resource "confluent_flink_statement" "keynote_flight_impact" {
  organization { id = data.confluent_organization.main.id }
  environment { id = local.core.confluent_environment_id }
  compute_pool { id = local.core.confluent_flink_compute_pool_id }
  principal { id = local.core.app_manager_service_account_id }
  rest_endpoint = local.rest_endpoint
  credentials {
    key    = local.core.app_manager_flink_api_key
    secret = local.core.app_manager_flink_api_secret
  }

  statement_name = "keynote-materialize-flight-impact-20260924"
  statement      = file("${path.module}/sql/25-serving-flight-impact.sql")
  properties     = local.sql_props

  lifecycle { ignore_changes = [statement] }

  depends_on = [confluent_flink_statement.keynote_passenger_risk]
}

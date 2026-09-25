# Flight recovery data model

**Legacy implementation.** The [keynote data schema](./keynote-data-gen-schemas-erd.md) now has executable SQL, a finite generator, and an app. The seven tables below describe the older pipeline that still exists in the deployed environment; they are not the keynote source schema.

The executable definitions are in [the Flink SQL directory](../terraform/airline-demo/sql/). This page explains how the tables fit together and where to change them. For deployment and demo steps, see the [walkthrough](./walkthrough.md).

## Data flow

```text
flight_updates ────────┐
                       ├─> passenger_journey ──> flight_ops_state ──> Lightning Tables ──> operations app
passenger_itineraries ─┘           │                       ▲
                                   │                gate_crew_status
                                   ├─> Lightning Tables ──> operations and passenger views
                                   ├─> RTCE/MCP ──> a connected coding agent
                                   ├─> recovery agent ──> passenger_recovery ──> Lightning Tables ──> app
                                   │          ▲                      │
                                   │   rebooking_inventory          └─> RTCE/MCP
                                   └─> Tableflow ──> Iceberg history
```

`passenger_journey` is the maintained business entity. It has one keyed row per synthetic passenger. Flink joins the itinerary to the inbound and connecting flight updates, then recalculates the row when either flight changes. The app reads `passenger_journey` and `passenger_recovery` through Lightning Queries. The separate RTCE/MCP path lets a connected coding agent query current context; the passenger view in the app does **not** use MCP.

Flink also maintains `flight_ops_state`, one row per connecting flight. It aggregates passenger risk and joins gate and crew status. Lightning Tables serves the resulting rows; it does not perform those joins or aggregates at query time.

## Tables

| Table | Key | Contents | Definition |
| --- | --- | --- | --- |
| `flight_updates` | Flight ID | Route, estimated arrival and departure, gate | [01-source-flight-updates.sql](../terraform/airline-demo/sql/01-source-flight-updates.sql) |
| `passenger_itineraries` | Passenger ID | Synthetic name, inbound and connecting flights, destination, bags, international flag | [02-source-passenger-itineraries.sql](../terraform/airline-demo/sql/02-source-passenger-itineraries.sql) |
| `rebooking_inventory` | Destination | Next flight, departure, hotel | [03-source-rebooking-inventory.sql](../terraform/airline-demo/sql/03-source-rebooking-inventory.sql) |
| `gate_crew_status` | Connecting flight ID | Crew duty margin, alternate gate, bag team availability | [04-source-gate-crew-status.sql](../terraform/airline-demo/sql/04-source-gate-crew-status.sql) |
| `passenger_journey` | Passenger ID | Name, connection, gate, connection math, bags, risk | [05-serving-passenger-journey.sql](../terraform/airline-demo/sql/05-serving-passenger-journey.sql) |
| `passenger_recovery` | Passenger ID | Agent proposal or executed action, type, status | [06-serving-passenger-recovery.sql](../terraform/airline-demo/sql/06-serving-passenger-recovery.sql) |
| `flight_ops_state` | Connecting flight ID | Passenger counts, minimum connection window, recommended action | [07-serving-flight-ops-state.sql](../terraform/airline-demo/sql/07-serving-flight-ops-state.sql) |

Every table has a `key STRING NOT NULL` primary key. Source keys are written by [the data generator](../scripts/airport_datagen.py); derived tables retain or compute keys in Flink. The three served tables use raw Kafka keys, upsert changelogs, and compacted topics. See their SQL definitions for the exact options and field types.

## Computation

### Passenger journey

[`passenger_journey`](../terraform/airline-demo/sql/05-serving-passenger-journey.sql) is a `CREATE OR ALTER MATERIALIZED TABLE` statement. Its two joins read the inbound arrival and connecting departure from `flight_updates`. A passenger is `MISS` when the estimated inbound arrival plus 30 minutes is later than the connecting departure. Otherwise, the row is `TIGHT` when the gap between arrival and departure is under 45 minutes; all other rows are `OK`. The table also derives `make_connection`, `minutes_to_departure`, `bag_status`, and `needs_recheck`.

These are demo rules, not airline operating policy. The source SQL defines the exact comparison operators and output columns.

### Flight operations

[`flight_ops_state`](../terraform/airline-demo/sql/07-serving-flight-ops-state.sql) is another materialized table. It groups `passenger_journey` by connecting flight, counts `MISS` and `TIGHT` passengers, then joins `gate_crew_status`. Its rule returns `NONE` when no connection is at risk, `HOLD` when crew duty margin is at least 15 minutes, `REGATE` when an alternate gate exists, and `PRIORITY_BAGS` otherwise. The app currently shows passengers and their recovery actions; it does not query this flight rollup.

### Recovery agent and approval

When Bedrock credentials are configured, Terraform creates the [model](../terraform/airline-demo/sql/10-model-recovery.sql), [agent](../terraform/airline-demo/sql/11-agent-recovery.sql), and [continuous recovery statement](../terraform/airline-demo/sql/12-insert-passenger-recovery.sql). The statement joins at-risk journeys to `rebooking_inventory`, converts inserts and updates to agent triggers with `TO_CHANGELOG`, and writes `PROPOSED` rows to `passenger_recovery`. A changed passenger risk can produce a replacement proposal for the same key. Agent output is generated text, so inspect the resulting action before approving it.

The app reads the proposal through Lightning Queries. Its **Approve** control writes an `EXECUTED` record to the same keyed Kafka topic; subsequent reads show that status. See [airport_app.py](../scripts/airport_app.py) for the read and write paths.

## Serving and history

| Consumer | Current implementation |
| --- | --- |
| Operations and passenger views | The FastAPI backend reads `passenger_journey` and `passenger_recovery` with Lightning Queries. |
| Connected coding agent | [`setup-rtce`](../scripts/setup_rtce.py) registers the RTCE MCP server for querying enabled topics. |
| Historical analysis | [Terraform](../terraform/airline-demo/main.tf) enables Tableflow with Confluent Managed Storage and the Iceberg format for `passenger_journey`, `passenger_recovery`, and `flight_ops_state` by default. |

The repository provisions the Iceberg tables but does not configure an external catalog integration or a query client. The [walkthrough](./walkthrough.md#6-open-history-with-tableflow) covers what you can verify after deployment.

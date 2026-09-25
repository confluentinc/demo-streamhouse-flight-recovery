# AGENTS.md

Instructions for AI coding agents and contributors working in this repository. `CLAUDE.md` is a symlink to this file.

## Project mission

This repository contains the older airline demo and the new keynote demo. The [keynote data schema](./docs/keynote-data-gen-schemas-erd.md) is the current narrative contract. The executable keynote SQL is in `terraform/airline-demo/sql/20` through `25`; `uv run airport-datagen` and `uv run airport-app` now run the keynote fixture and app. `CLAUDE.md` links to this file.

## Keynote direction

* Use the current script's three demo scenes: live flight and passenger queries, Flink connection risk with hotel-aware recovery, then Tableflow to Iceberg and AWS Glue/Athena for a historical question.
* Use three source feeds: `flight_status`, three-column `passenger_connections`, and `hotel_inventory`. The connection name avoids the deployed seven-column legacy `passenger_itineraries` topic. Keep `keynote` out of topic names. Add a field only when a named scene, calculation, or query needs it.
* Keep the hotel availability change and the passenger recovery visible. The keynote generator currently emits two deterministic offers; the app checks hotel inventory through Lightning before selection and booking. The planned agent uses a model hosted in Confluent and reads hotel context through RTCE/MCP. Do not present the older Bedrock agent as that keynote flow.
* Treat the older `passenger_journey`, gate/crew, rebooking-inventory, approval, and Maya pivot story as the current implementation or historical planning, depending on the document. They are not the approved keynote script.
* Use the [keynote data schema](./docs/keynote-data-gen-schemas-erd.md) and [simple architecture diagram](./docs/keynote-architecture.excalidraw) as the keynote design references before changing executable schemas.
* Keep the keynote generator behind `uv run airport-datagen`. `uv run deploy --with-datagen` runs the same finite sequence after provisioning. Use targeted Terraform apply for incremental changes to an existing deployment.

## Technical boundaries

* Run joins, aggregations, enrichment, and complex computation in Flink, not Lightning Tables.
* The app reads current state through Lightning Tables; connected agents can query it through RTCE/MCP.
* In this deployment, an unfiltered Lightning scan returned only 200 rows even with `LIMIT 500`. Query passenger offers by `passenger_id` and booked offers by `status` so the app does not silently omit later passengers.
* The current deployment uses Tableflow with managed Iceberg storage on older topics. The keynote script calls for Iceberg in S3 with AWS Glue/Athena; that integration is planned, not deployed here. No Webhooks source connector is deployed yet; the generator publishes hotel rows directly.
* Use synthetic data only; no real passenger/customer PII.
* Keep secrets and internal material out of tracked files.

## Repository layout

* [`scripts/`](./scripts/) — one flat Python package: deploy/destroy, credential and Terraform helpers, RTCE setup, keynote generator/app, and retained legacy generator/app.
* [`terraform/core/`](./terraform/core/) — Confluent Cloud environment, cluster, Flink pool, service accounts, keys, optional Bedrock connection.
* [`terraform/airline-demo/`](./terraform/airline-demo/) — the demo pipeline: source tables, maintained state, serving tables, recovery streaming agent (Flink SQL in `sql/`). Separate root because its provider reads core's outputs.
* [`tests/`](./tests/) — pytest + `node --test` suites.
* [`docs/`](./docs/) — walkthrough, legacy data model, and keynote data contract.

Entry points are defined in [`pyproject.toml`](./pyproject.toml) (`uv run deploy`, `uv run airport-datagen`, `uv run airport-app`, ...). Add to an existing module before creating a new directory.

## Implementation rules

* Use `uv` for everything Python (`uv sync`, `uv run ...`); never bare `pip`.
* Every data generator must accept a seed and controllable clock.
* Provide idempotent deploy, reset, validate, and teardown paths.
* Keep external integrations behind adapters so the core scenario can run deterministically.
* Store no credentials in source, docs, sample output, terminal history, screenshots, or recordings. `credentials.env` is gitignored.
* Prefer readable demo code over generalized framework code. Optimize for a clean first run, not maximal configurability; don't prompt for values the tooling can choose itself.
* Use business-readable names for topics, tables, fields, agents, and actions. Avoid unexplained acronyms in UI.
* In markdown, link to other local files with relative paths.
* Name new files and directories kebab-case (Python modules excepted: snake_case). No spaces, parentheses, or uppercase.
* Before changing Terraform, connectors, or deployment configuration, read existing resource names, environment variables, credentials handling, and naming conventions. Use targeted Terraform apply for targeted changes.

## Validation checklist

Before merging a change:

* Do the docs match the current SQL and app paths?
* Does the app read through Lightning Tables and the connected agent through RTCE/MCP?
* Can the demo run without a fragile third-party dependency?
* Can someone outside the core team deploy and understand it from the README?
* `uv run pytest`, `node --test tests/test-app-pivot.js`, and `uv run ruff check scripts tests` pass.

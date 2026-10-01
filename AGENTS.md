# AGENTS.md

Instructions for AI coding agents and contributors working in this repository. `CLAUDE.md` is a symlink to this file.

## Project mission

This repository is the River Air flight recovery demo on Confluent Cloud. The demo script is the source of truth for the story; the [data model](./docs/data-model.md) is the data contract that serves it. The Flink SQL is in `terraform/airline-demo/sql/20` through `32`; `uv run airport-datagen` and `uv run airport-app` run the generator and app.

## Demo direction

* Use the script's three demo scenes: live flight and passenger queries, Flink connection risk with hotel-aware recovery, then Tableflow to Iceberg and AWS Glue/Athena for the historical questions.
* Work backwards from those questions (delayed flights, impacted passengers, and hotel cost in the last 30 days; average recovery time). Add a field only when a named scene, calculation, or query needs it.
* Use three source feeds: `flight_status`, `passenger_itineraries` (five columns: the inbound and connecting flights and a seat on each; the connecting columns are null when the trip ends at SFO), and `hotel_inventory`. Every flight uses one 182-seat layout modeled after a two-class Boeing 737-900ER (`SEATS` in the generator), so no flight carries more than 182 passengers. Flink produces `passenger_state` and `flight_impact`; offers go to `passenger_recommendations`.
* Keep the hotel availability change and the passenger recovery visible. Each HIGH-risk passenger gets two offers; the app checks hotel inventory through Lightning before selection and booking. The recovery agent (`sql/26`–`31`) writes the live day's offers for RA417, the flight the demo follows; the generator writes every other flight's. It uses Claude on Amazon Bedrock, because Streaming Agents don't support the Confluent-hosted models, and reads flights and hotels through RTCE/MCP. `sql/32` creates a Native Inference model, `passenger_recovery_mode1`, for the video only: nothing uses it until Native Inference supports tool calling. The agent needs core's Bedrock connection; without that, and for history days, the generator writes the same offers. `AI_RUN_AGENT` reads only append-only input, so it reads the `passenger_state_changes` and `impacted_passengers` staging tables rather than the upsert `passenger_state`. A Flink statement can hold only one `TO_CHANGELOG`.
* Recovery time is `recommended_at - impacted_at`; it excludes the customer's decision time.
* Keep the generator behind `uv run airport-datagen`. `uv run deploy` publishes the same data, starts the 90-minute live stream in the background, then enables Lightning Tables and RTCE. People run only `uv run deploy`; its `--automated`/`--testing` flags are for bots. Use targeted Terraform apply for incremental changes to an existing deployment.

## Technical boundaries

* Run joins, aggregations, enrichment, and complex computation in Flink, not Lightning Tables.
* The app reads current state through Lightning Tables; connected agents can query it through RTCE/MCP. Flink output reaches Lightning only at its once-a-minute commit, so the app reads flight status and delay from the `flight_status` source and uses `flight_impact` for at-risk counts.
* Every Lightning query returns at most 200 rows, even with `LIMIT 500` or a filter, and OFFSET, COUNT, and GROUP BY are rejected. Keep one service day at 180 flights so a single windowed query returns it, and page anything larger by key (`WHERE "KEY" > 'last' ORDER BY "KEY" LIMIT 200`).
* Tableflow writes Iceberg to a customer-owned S3 bucket with an AWS Glue catalog when AWS credentials are present (`terraform/airline-demo/analytics.tf`). No Webhooks source connector is deployed yet; the generator publishes hotel rows directly.
* Use synthetic data only; no real passenger/customer PII.
* Keep secrets and internal material out of tracked files.

## Repository layout

* [`scripts/`](./scripts/) — one flat Python package: deploy/destroy, credential and Terraform helpers, RTCE setup, the generator (`airport_datagen.py`), and the app (`airport_app.py`, `static/`).
* [`terraform/core/`](./terraform/core/) — Confluent Cloud environment, cluster, Flink pool, service accounts, keys, optional Bedrock connection.
* [`terraform/airline-demo/`](./terraform/airline-demo/) — the demo pipeline (Flink SQL in `sql/`) and Tableflow to S3/Glue. Separate root because its provider reads core's outputs.
* [`tests/`](./tests/) — pytest suites.
* [`docs/`](./docs/) — walkthrough, data model, and architecture diagram.

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
* `uv run pytest` and `uv run ruff check scripts tests` pass.

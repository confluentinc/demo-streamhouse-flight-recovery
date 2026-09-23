# AGENTS.md

Instructions for AI coding agents and contributors working in this repository. `CLAUDE.md` is a symlink to this file.

## Project mission

This repository implements a synthetic airline disruption demo. An inbound delay changes passengers' connection risk, and the app displays their current state and recovery proposals.

## Technical boundaries

* Run joins, aggregations, enrichment, and complex computation in Flink, not Lightning Tables.
* The app reads current state through Lightning Tables; connected agents can query it through RTCE/MCP.
* Tableflow materializes Iceberg tables for historical analysis.
* Use synthetic data only; no real passenger/customer PII.
* Keep secrets and internal material out of tracked files.

## Repository layout

* [`scripts/`](./scripts/) — one flat Python package: deploy/destroy, credential and Terraform helpers, RTCE setup, the seeded data generator, and the airline app (`airport_app.py` + `static/`).
* [`terraform/core/`](./terraform/core/) — Confluent Cloud environment, cluster, Flink pool, service accounts, keys, optional Bedrock connection.
* [`terraform/airline-demo/`](./terraform/airline-demo/) — the demo pipeline: source tables, maintained state, serving tables, recovery streaming agent (Flink SQL in `sql/`). Separate root because its provider reads core's outputs.
* [`tests/`](./tests/) — pytest + `node --test` suites.
* [`docs/`](./docs/) — public walkthrough and data model.

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

## Validation checklist

Before merging a change:

* Do the docs match the current SQL and app paths?
* Does the app read through Lightning Tables and the connected agent through RTCE/MCP?
* Can the demo run without a fragile third-party dependency?
* Can someone outside the core team deploy and understand it from the README?
* `uv run pytest`, `node --test tests/test-app-pivot.js`, and `uv run ruff check scripts tests` pass.

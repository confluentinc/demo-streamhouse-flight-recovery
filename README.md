# Streamhouse Flight Recovery Demo

An inbound flight slips, and 12 synthetic connecting passengers face different problems. This demo shows a **Streamhouse** on Confluent Cloud. Flink maintains current state for every passenger's journey. That state then serves:

- **an operations app** through [Lightning Tables](https://docs.confluent.io/cloud/current/lightning/overview.html)
- **AI agents** through a [Streaming Agent](https://docs.confluent.io/cloud/current/ai/streaming-agents/overview.html) and the [Real-Time Context Engine](https://docs.confluent.io/cloud/current/ai/real-time-context-engine/overview.html) (MCP)
- **open analytics** through [Tableflow](https://docs.confluent.io/cloud/current/topics/tableflow/overview.html) (Iceberg in this deployment)

## Quickstart

Prerequisites: `uv`, `terraform`, the Confluent CLI, and a Confluent Cloud account. AWS Bedrock access is optional and powers the recovery agent. The full setup is in the [walkthrough](./docs/walkthrough.md#prerequisites).

```bash
uv run deploy                      # Confluent Cloud environment + demo pipeline (AWS us-east-1)
uv run setup-rtce                  # enable RTCE + Lightning Tables, register MCP with your coding agent
uv run airport-datagen             # seed passengers, then slip inbound flight JA417
uv run airport-app                 # ops + passenger app at http://127.0.0.1:8000
uv run airport-datagen --phase pivot   # live change: Maya's plan updates on screen
uv run destroy                     # tear everything down
```

Deployment, Tableflow status checks, troubleshooting, and reset are in the [walkthrough](./docs/walkthrough.md). The [data model](./docs/data-model.md) explains the tables and links to the executable Flink SQL.

## The app

`uv run airport-app` serves one FastAPI backend with two views over the same maintained entity:

- **Operations:** every at-risk passenger and the recovery agent's recommended action, with an **Approve** control. Approving produces an `EXECUTED` recovery record back through Kafka, and it appears on the next Lightning Tables read. That closes the loop through the platform.
- **Passenger:** one passenger's live status and recovery offer.

The browser polls every 2.5 seconds. If a proposed action changes before approval, a banner shows the old and new plans. The serving API key stays server-side.

## Repository layout

| Path | What's there |
|---|---|
| [`scripts/`](./scripts/) | Deploy/destroy, RTCE setup, seeded data generator, and the app (`airport_app.py` + `static/`) |
| [`terraform/core/`](./terraform/core/) | Environment, Kafka cluster, Flink compute pool, service accounts, keys |
| [`terraform/airline-demo/`](./terraform/airline-demo/) | The demo pipeline. The Flink SQL in [`sql/`](./terraform/airline-demo/sql/) is the source of truth |
| [`tests/`](./tests/) | `uv run pytest` and `node --test tests/test-app-pivot.js` |
| [`docs/`](./docs/) | Walkthrough and data model |

Contributors and AI coding agents should read [AGENTS.md](./AGENTS.md).

All data is synthetic. Never commit `credentials.env`.

Licensed under Apache 2.0. See [LICENSE](./LICENSE) and [CHANGELOG.md](./CHANGELOG.md).

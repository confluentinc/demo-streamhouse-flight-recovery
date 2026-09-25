# Airport Disruption Recovery — Streamhouse Walkthrough

> **Keynote demo:** `uv run airport-datagen` and `uv run airport-app` now run the [keynote data schema](./keynote-data-gen-schemas-erd.md). The older JA417/passenger-journey walkthrough below remains as a legacy reference; use the explicit Python modules shown there.

## Keynote demo run

For a fresh deployment, `uv run deploy --with-datagen` provisions the tables and runs the finite fixture. Run `uv run setup-rtce` for Lightning Tables access, then `uv run airport-app` to open the operations and passenger view. A separate `uv run airport-datagen` replays the same sequence with 200 passengers, two offers each, and a hotel sellout. Use `--phase seed`, `delay`, `offers`, or `sellout` to rehearse a scene; `--dry-run` prints the records and `--reset` writes tombstones for that seed's keys.

The live keynote demo uses `flight_status`, `passenger_connections`, `hotel_inventory`, `passenger_risk`, `flight_impact`, and `passenger_recommendations`. The seven-column `passenger_itineraries` topic below belongs to the legacy flow. The hosted recovery agent, Webhooks source connector, and S3/Glue/Athena scene are still planned.

## Legacy JA417 walkthrough

In this demo, one inbound flight slips. Flink maintains each passenger's current journey; the app
reads it through Lightning Tables, and a connected agent can query it through RTCE/MCP. Tableflow
materializes the served topics as Iceberg tables. Built on [Confluent Cloud for Apache Flink](https://docs.confluent.io/cloud/current/flink/overview.html),
[Lightning Tables](https://docs.confluent.io/cloud/current/lightning/overview.html),
[RTCE](https://docs.confluent.io/cloud/current/ai/real-time-context-engine/overview.html), and
[Tableflow](https://docs.confluent.io/cloud/current/topics/tableflow/overview.html).

### What This System Does

One incident (flight **JA417** slips) puts 12 synthetic connections at risk. The platform:

1. **Maintains current state** – Flink turns scattered flight/reservation/crew events into one
   governed table, `passenger_journey`, computing each passenger's `risk` (MISS / TIGHT / OK) with
   legible SQL — no ML black box.
2. **Serves apps and agents** – the ops app reads live state over [Lightning Tables](https://docs.confluent.io/cloud/current/lightning/overview.html);
   an AI agent reads the *same* state over [RTCE/MCP](https://docs.confluent.io/cloud/current/ai/real-time-context-engine/overview.html).
3. **Acts, then opens history** – a [Streaming Agent](https://docs.confluent.io/cloud/current/ai/streaming-agents/overview.html)
   drafts each passenger's recovery; an operator approves it (write-back to Kafka); and
   [Tableflow](https://docs.confluent.io/cloud/current/topics/tableflow/overview.html) lands it all
   in open Iceberg for analysis.

The data model and Flink SQL are explained in [`data-model.md`](./data-model.md).

## Prerequisites

### Local dependencies

```bash
brew install uv git python && brew tap hashicorp/tap && brew install hashicorp/tap/terraform && brew install --cask confluent-cli
```

**Windows:**

```powershell
winget install astral-sh.uv Git.Git Hashicorp.Terraform ConfluentInc.Confluent-CLI Python.Python
```

### API keys & access

> [!NOTE]
>
> This repository deploys to AWS `us-east-1`.

- **Confluent Cloud** account with rights to create environments, clusters, Flink pools, and API keys.
- **AWS Bedrock** (optional) powers the recovery streaming agent. Run `uv run api-keys create` to
  auto-generate credentials. Without Bedrock, the topic/serving/ops stack still deploys — add the
  agent later.

> [!WARNING]
>
> **AWS Bedrock users:** request access to the Claude model in `us-east-1` via the [Model Catalog](https://console.aws.amazon.com/bedrock/home#/model-catalog) before deploying.

## Deploy the Demo

Clone and pull the latest:

```bash
git clone https://github.com/confluentinc/demo-streamhouse-flight-recovery.git
cd demo-streamhouse-flight-recovery
uv sync
```

Deploy the platform (`terraform/core` then `terraform/airline-demo` — cluster, Flink pool, the 7
tables, the maintained-state SQL, and the recovery agent when Bedrock creds are present):

```bash
uv run deploy
```

It prompts for your Confluent Cloud login + API key and (optionally) AWS Bedrock credentials.

Then enable RTCE + Lightning and register the MCP server with your coding agent:

```bash
uv run setup-rtce --client claude    # or: codex | gemini
```

> [!NOTE]
>
> This mints an org-wide (Global) API key for the reader service account and writes it to the
> git-ignored `credentials.env`. It's the key the app and Lightning queries authenticate with —
> keep it out of screenshots and recordings.

## Usecase Walkthrough

### 1. Generate the incident

```bash
uv run python -m scripts.airport_datagen  # seed (all OK), wait, then slip JA417
```

Inbound **JA417 (ORD→SFO)** slips **+35 min**; 12 passengers on 4 onward flights out of SFO flip to
**9 MISS / 3 TIGHT**. Deterministic: `--seed` controls jitter, and `--now` rebases the clock. Wait
for Flink to recompute `passenger_journey` and, when configured, for the agent to write `passenger_recovery`.

Other phases: `--phase seed` (steady state), `--phase slip` (fire the cascade), `--phase pivot`
(replace the PDX recovery option), `--reset` (delete-records the 4 source topics), and `--dry-run`
(print, connect to nothing).

### 2. Trigger the live-state plot twist

Select Maya Chen (`P-1009`) in the app while her JA540/Kimpton overnight proposal is visible, then run:

```bash
uv run python -m scripts.airport_datagen --phase pivot
```

JA512 moves 25 minutes later. Flink recomputes Maya's connection window from 11 to 36 minutes and
changes Current Passenger State from MISS to TIGHT. That update retriggers the streaming agent, which
replaces the overnight REBOOK plan with an EXPEDITE plan on Maya's original flight. The app polls
Lightning Tables every 2.5 seconds; once the replacement lands, it selects Maya and shows both actions
in a **Current-state change detected** banner. Approve only after EXPEDITE appears.

### 3. Serve the operation (Lightning Tables + act)

```bash
uv run python -m scripts.airport_app  # http://127.0.0.1:8000
```

Left = **Operations**: every at-risk passenger + the recommended recovery, each with **Approve**.
Right = **Passenger**: one passenger's live status + concierge offer. The read path is Lightning
Queries (the Global key stays server-side). Click **Approve** on a MISS row — it produces an
`EXECUTED` record back to `passenger_recovery`, which shows up on the next Lightning read: a closed
loop through the platform. You should see the top-bar counts read `12 · 9 · 3 · 0`, then the
**Executed** count climb as you approve.

### 4. Serve the agent (RTCE / MCP)

With the MCP server registered (above), ask your coding agent to read the live context. RTCE is
read-only (no JOINs/aggregations/mutations — do those upstream in Flink):

- `listTopics` — discover the materialized tables.
- `getMetadata(topic_name="passenger_journey")` — inspect the schema.
- `queryData(topic_name="passenger_journey", query="…", max_result_rows=N)` — read live rows.

This is a separate agent-context path. The passenger view in `airport-app` reads Lightning
Queries through its backend; a connected coding agent can query the same enabled topics over MCP.

### 5. Query current state over Lightning directly (optional)

Print a ready-to-run Lightning Queries `curl` for any topic:

```bash
uv run setup-rtce --lightning passenger_journey                 # scan (LIMIT 10)
uv run setup-rtce --lightning passenger_journey --key P-1009    # one passenger (point lookup)
```

> [!WARNING]
>
> The printed command embeds the Global key. Redirect it to a git-ignored file — don't paste it
> into a shared terminal or recording.

### 6. Open history with Tableflow

Tableflow enablement is codified in `terraform/airline-demo` (`enable_tableflow`, `tableflow_topics`)
using **Confluent Managed Storage** — no S3 bucket or IAM to configure. Verify:

```bash
confluent tableflow topic list --cluster <lkc-…>
```

All three of `passenger_journey`, `passenger_recovery`, `flight_ops_state` should show `RUNNING`,
`ICEBERG`, storage `MANAGED`. Current operations stay on Lightning/RTCE; open history lives here.

This repository provisions the Iceberg tables; the command above checks their status. It does not configure
Databricks, Glue, or an Iceberg query client. To run historical queries, connect a compatible client
to the managed tables using the [Tableflow documentation](https://docs.confluent.io/cloud/current/topics/tableflow/overview.html).
An external catalog integration requires additional storage and catalog configuration beyond this demo.

> [!NOTE]
>
> The Tableflow key is minted on the `app-manager` service account. The RTCE Global key **cannot**
> enable Tableflow (its reader SA only has `DeveloperRead`).

### Reset between runs

```bash
uv run python -m scripts.airport_datagen --phase all
```

Re-runs the scenario and clears `EXECUTED` approvals (the agent re-proposes once each passenger is
at-risk again). Wait for counts to settle back to `12 · 9 · 3 · 0`.

## Conclusion

The demo maintains passenger state in Flink, serves current rows to the app through Lightning
Tables and to a connected agent through RTCE/MCP, and enables Tableflow for Iceberg history.

## Troubleshooting

<details>
<summary>Click to expand</summary>

- **App shows `503 … No RTCE/Lightning Global API key`** — run `uv run setup-rtce`; it writes the
  Global key to `credentials.env`.
- **`/api/state` empty or all-OK** — datagen hasn't slipped yet, or Flink is catching up. Run
  `uv run python -m scripts.airport_datagen --phase all` and wait for the updated rows.
- **`passenger_recovery` empty (no recommended actions)** — Bedrock creds weren't provided at
  deploy, so the agent was skipped. Re-deploy with Bedrock creds.
- **Lightning `curl` / app read returns 401** — Global key still propagating (retry), or the topic
  lacks `DeveloperRead` for the reader SA.
- **`queryData` errors on missing args** — pass **all** of `topic_name`, `query`, and
  `max_result_rows`. `getMetadata` takes a single `topic_name` string.
- **Tableflow topics not listed** — ensure `terraform/airline-demo` applied with
  `enable_tableflow = true`.
- **Targeted Terraform change** — never `apply` the full stack for a targeted change; use `-target`.

</details>

## 🧹 Clean-up

```bash
uv run destroy
```

Removes the Terraform-managed resources (core + airline-demo, including Tableflow and minted keys).
Confirm the environment is gone in the Confluent Cloud console, and rotate any credentials used for
a recording. Synthetic data only — no real PII; never commit secrets.

## Navigation

- **Overview:** [README](../README.md)
- **Data model + Flink SQL:** [data-model.md](./data-model.md)

"""Reset Confluent Cloud to where Demo 2 (video 2) starts, so its statements can run live on screen.

Video 1's objects stay: the source tables, passenger_state, flight_impact, Lightning Tables, RTCE,
and the Bedrock model the agent runs on (passenger_recovery_model). The reset

  1. stops the data stream and every statement that creates or runs a Demo 2 object,
  2. drops the Native Inference model, the RTCE tool, the agent, and the two staging tables,
  3. rebuilds the staging tables off screen (the agent's INSERT reads them),
  4. deletes RA417's offers for today, and
  5. restarts the stream, so RA417's delay and the Harbor Hotel sellout play out again.

Then CREATE MODEL, CREATE TOOL, CREATE AGENT, and the AI_RUN_AGENT INSERT run live on screen
(see the SQL for each in docs/demo-script-screens.md). Running the reset again is safe.

Usage:
  uv run reset-demo-2            # reset to the start of Demo 2
  uv run reset-demo-2 --restore  # skip the live run: start the agent the way deploy does
"""

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import timedelta

from dotenv import dotenv_values

from scripts import airport_datagen, deploy
from scripts.login_checks import ensure_confluent_login
from scripts.terraform import get_project_root, run_terraform_output
from scripts.terraform_runner import run_terraform

# Demo 2 creates these on screen. passenger_recovery_model stays: the agent runs on it.
DROPS = (
    "DROP AGENT IF EXISTS passenger_recovery_agent",
    "DROP TOOL IF EXISTS live_context",
    "DROP MODEL IF EXISTS passenger_recovery_mode1",
    "DROP MATERIALIZED TABLE IF EXISTS impacted_passengers",
    "DROP MATERIALIZED TABLE IF EXISTS passenger_state_changes",
)
# A statement whose SQL names one of these creates or runs a Demo 2 object, whether Terraform
# or an earlier take on screen created it.
DEMO_2_NAMES = ("AI_RUN_AGENT", "passenger_recovery_agent", "live_context", "passenger_recovery_mode1",
                "passenger_state_changes", "impacted_passengers")
# Terraform's copies of the statements the reset deletes; removed from state so a later apply recreates them.
STATE_ADDRESSES = (
    'confluent_flink_statement.agent_setup["native_model"]',
    'confluent_flink_statement.agent_setup["tool"]',
    "confluent_flink_statement.recovery_agent[0]",
    "confluent_flink_statement.recovery_offers[0]",
    "confluent_flink_statement.passenger_state_changes[0]",
    "confluent_flink_statement.impacted_passengers[0]",
)
STAGING_STATEMENTS = ("airport-agent-passenger-state-changes", "airport-agent-impacted-passengers")
STAGING_TARGETS = (
    "confluent_flink_statement.passenger_state_changes",
    "confluent_flink_statement.impacted_passengers",
)


class Flink:
    """Runs and deletes Flink statements with the Confluent CLI, as the app-manager service account."""

    def __init__(self, core: dict):
        self.scope = ["--cloud", "aws", "--region", core["cloud_region"],
                      "--environment", core["confluent_environment_id"]]
        self.pool = core["confluent_flink_compute_pool_id"]
        self.principal = core["app_manager_service_account_id"]
        self.properties = (f"sql.current-catalog={core['confluent_environment_display_name']},"
                           f"sql.current-database={core['confluent_kafka_cluster_display_name']}")

    def _cli(self, *args: str) -> subprocess.CompletedProcess:
        return subprocess.run(["confluent", "flink", "statement", *args, *self.scope],
                              capture_output=True, text=True, timeout=300)

    def statements(self) -> list[dict]:
        result = self._cli("list", "-o", "json")
        if result.returncode != 0:
            raise RuntimeError(f"Could not list Flink statements: {result.stderr.strip()}")
        statements = json.loads(result.stdout or "[]")
        if statements and not all("name" in s and "statement" in s for s in statements):
            raise RuntimeError("`confluent flink statement list -o json` no longer returns name and statement")
        return statements

    def delete(self, name: str) -> None:
        self._cli("delete", name, "--force")

    def run(self, sql: str) -> None:
        """Run one DDL statement to completion, then delete it so it doesn't clutter the statement list."""
        name = f"reset-demo-2-{int(time.time() * 1000)}"
        created = self._cli("create", name, "--sql", sql, "--compute-pool", self.pool,
                            "--service-account", self.principal, "--property", self.properties, "--wait")
        described = self._cli("describe", name, "-o", "json")
        status = json.loads(described.stdout or "{}").get("status", "") if described.returncode == 0 else ""
        self.delete(name)
        if created.returncode != 0 or status != "COMPLETED":
            detail = created.stderr.strip() or created.stdout.strip()
            raise RuntimeError(f"{sql} ended {status or 'with an error'}: {detail}")


def demo_2_statements(statements: list[dict]) -> list[str]:
    """Names of the statements that create or run a Demo 2 object, the INSERT first."""
    found = [s for s in statements if any(n in (s.get("statement") or "") for n in DEMO_2_NAMES)]
    found.sort(key=lambda s: "AI_RUN_AGENT(" not in (s.get("statement") or ""))  # comments mention it too
    return [s["name"] for s in found]


def ra417_offer_keys(plan: airport_datagen.Plan) -> list[str]:
    """Every offer key the agent can write for today's RA417."""
    hero = plan.live.hero
    return sorted(f"{p.key}-O{n}" for p in plan.live.passengers if p.inbound is hero and p.connecting for n in (1, 2))


def _forget_in_terraform(demo, keep_staging: bool = False) -> None:
    listed = subprocess.run(["terraform", "state", "list"], cwd=demo, capture_output=True, text=True)
    present = set(listed.stdout.split())
    addresses = [a for a in STATE_ADDRESSES if a in present
                 and not (keep_staging and any(t in a for t in STAGING_TARGETS))]
    if addresses:
        subprocess.run(["terraform", "state", "rm", *addresses], cwd=demo, check=True, capture_output=True)


def reset(root, flink: Flink) -> None:
    demo = root / "terraform" / "airline-demo"
    print("Stopping the data stream...")
    airport_datagen.stop_previous_stream()

    # The INSERT stops first; the staging tables' own statements go after DROP stops their queries.
    names = demo_2_statements(flink.statements())
    print(f"Deleting {len(names)} Demo 2 statement(s): {', '.join(names) or 'none'}")
    for name in names:
        if name not in STAGING_STATEMENTS:
            flink.delete(name)
    for sql in DROPS:
        print(f"  {sql}")
        flink.run(sql)
    for name in names:
        if name in STAGING_STATEMENTS:
            flink.delete(name)
    _forget_in_terraform(demo)

    print("Rebuilding the staging tables off screen...")
    os.environ["TF_VAR_enable_recovery_agent"] = "true"
    if not run_terraform(demo, targets=STAGING_TARGETS):
        raise RuntimeError("Rebuilding passenger_state_changes and impacted_passengers failed")

    start = airport_datagen._clock(None)
    keys = ra417_offer_keys(airport_datagen.build_plan(start, history=False))
    publisher = airport_datagen.Publisher(airport_datagen.extract_kafka_credentials("aws", root), dry_run=False)
    for key in keys:
        publisher.publish(airport_datagen.OFFERS, key, None)
    publisher.flush()
    print(f"Deleted {len(keys)} RA417 offer keys for today")

    print("Restarting the stream (today's flights only; history is unchanged)...")
    clock = deploy._run_datagen(root, agent=True, history=False)

    def at(minutes: float) -> str:
        return f"{clock + timedelta(minutes=minutes):%H:%M}"

    print(f"""
✓ Ready for Demo 2. The stream started at {at(0)} UTC:
  {at(6)} to {at(16.5)}  RA417's passengers turn HIGH (record 2.1 here)
  {at(40)}           Harbor Hotel sells out (record 2.4 from {at(38)})
Run CREATE MODEL, CREATE TOOL, CREATE AGENT, then the AI_RUN_AGENT INSERT on screen.""")


def restore(root, flink: Flink) -> None:
    """Start the agent the way deploy does, after removing any copy started on screen."""
    for name in demo_2_statements(flink.statements()):
        if name not in STAGING_STATEMENTS:
            flink.delete(name)
    _forget_in_terraform(root / "terraform" / "airline-demo", keep_staging=True)
    deploy._start_agent(root)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="uv run reset-demo-2", description=__doc__.split("\n")[0])
    parser.add_argument("--restore", action="store_true", help="start the agent the way deploy does instead")
    parser.add_argument("--yes", action="store_true", help="skip the confirmation prompt")
    args = parser.parse_args(argv)

    root = get_project_root()
    core_state = root / "terraform" / "core" / "terraform.tfstate"
    if not core_state.exists():
        sys.exit("No deployment found. Run `uv run deploy` first.")
    core = run_terraform_output(core_state)
    if not core.get("bedrock_enabled"):
        sys.exit("This deployment has no Bedrock connection, so Demo 2's agent can't run. Nothing to reset.")
    ensure_confluent_login(dotenv_values(root / "credentials.env"))
    flink = Flink(core)

    if args.restore:
        restore(root, flink)
        return
    print(f"This drops Demo 2's agent, tool, Native Inference model, and staging tables in "
          f"{core['confluent_environment_display_name']}, deletes RA417's offers, and restarts the stream.")
    if not args.yes and input("Continue? (y/n): ").strip().lower() != "y":
        sys.exit("Cancelled.")
    try:
        reset(root, flink)
    except RuntimeError as error:
        sys.exit(f"✗ {error}")


if __name__ == "__main__":
    main()

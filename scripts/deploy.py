#!/usr/bin/env python3
"""
Deploy the Streamhouse airline flight-recovery demo to Confluent Cloud.

Always runs, in order:
  1. terraform/core        — environment, cluster, Flink pool, service accounts,
                             API keys, RTCE reader, (optional) Bedrock connection.
  2. terraform/airline-demo — the airline Flink pipeline (tables, maintained state,
                             ops spoke, and the recovery streaming agent when
                             Bedrock creds are present).
  3. The keynote data generator (same sequence as `uv run airport-datagen`).
  4. Lightning Tables and RTCE on the demo topics, plus the RTCE MCP server for
     the coding agent chosen up front (same as `uv run setup-rtce`).

AWS-only by design (RTCE + Flink Native Inference are AWS-only), so there is no
cloud or region choice. Resource names use TF_VAR_resource_prefix from
credentials.env, or DEFAULT_PREFIX when unset (saved on first run so later
runs keep the same names).

Usage:
  uv run deploy               # interactive; the only command a person needs
  uv run deploy --automated   # bots: non-interactive from credentials.env, registers Claude Code
  uv run deploy --testing     # bots: non-interactive from credentials.env, no MCP registration
"""

import argparse
import getpass
import os
import sys
import time

from dotenv import dotenv_values, set_key

from scripts import setup_rtce
from scripts.credentials import (
    generate_confluent_api_keys,
    load_or_create_credentials_file,
)
from scripts.login_checks import _attempt_login_quiet, ensure_confluent_login
from scripts.terraform import get_project_root, run_terraform_output
from scripts.terraform_runner import run_terraform
from scripts.tfvars import write_tfvars_for_deployment
from scripts.ui import prompt_with_default

DEFAULT_REGION = "us-east-1"  # RTCE-supported; Bedrock Claude available here
DEFAULT_PREFIX = "flight-recovery"
DEPLOY_TARGETS = ["core", "airline-demo"]

# Module-level buffer for Plan B fallback when set_key() fails (e.g. Windows locks).
_pending_writes: dict = {}
_write_failed: bool = False


def _detect_ambient_aws_credentials() -> tuple[str, str, str] | None:
    """Resolve AWS credentials via boto3's standard chain (env vars, `aws sso
    login` cache, shared config/profile, IMDS, ...). Returns
    (access_key, secret_key, session_token) — session_token is "" when the
    credentials aren't temporary — or None if nothing resolves.

    Preferred over asking the user to paste one in: a temporary session
    token is a long (~300-400 char) string, and pasting that into a plain
    terminal prompt can freeze some terminals. Most people running this
    already have valid AWS credentials in their shell (SSO login, env vars,
    a profile), so this picks those up directly instead.
    """
    try:
        import boto3

        resolved = boto3.Session().get_credentials()
        if resolved is None:
            return None
        frozen = resolved.get_frozen_credentials()
        if not frozen.access_key or not frozen.secret_key:
            return None
        return frozen.access_key, frozen.secret_key, frozen.token or ""
    except Exception:
        return None


def _save_env_safe(creds_file, key: str, value: str) -> None:
    """Write a key to credentials.env with retry + in-memory fallback."""
    global _write_failed
    if _write_failed:
        _pending_writes[key] = value
        return
    for attempt in range(3):
        result = set_key(str(creds_file), key, value)
        if result[0] is not None:
            check = dotenv_values(str(creds_file))
            if check.get(key) == value:
                return
        if attempt < 2:
            time.sleep(0.3 * (attempt + 1))
    _write_failed = True
    _pending_writes[key] = value


def _flush_pending_writes(creds_file) -> None:
    """Bulk-write any buffered credentials at the end of the flow (Plan B)."""
    if not _pending_writes:
        return
    try:
        existing = dotenv_values(str(creds_file)) if creds_file.exists() else {}
        existing.update(_pending_writes)
        with open(creds_file, "w", encoding="utf-8", newline="\n") as f:
            for k, v in existing.items():
                f.write(f"{k}='{v}'\n")
        check = dotenv_values(str(creds_file))
        for k, v in _pending_writes.items():
            if check.get(k) != v:
                raise ValueError(f"Read-back verification failed for {k}")
    except Exception as e:
        print(f"\n✗ Could not write credentials.env: {e}", file=sys.stderr)
        sys.exit(1)


def _deploy(root, targets) -> None:
    print("\n=== Starting Deployment ===")
    for env in targets:
        env_path = root / "terraform" / env
        if not env_path.exists():
            print(f"Warning: {env_path} does not exist, skipping.")
            continue
        if not run_terraform(env_path):
            print(f"\nDeployment failed at {env}. Stopping.")
            sys.exit(1)
    print("\n✓ All deployments completed successfully!")


def main():
    parser = argparse.ArgumentParser(description="Deploy the Streamhouse airline demo")
    parser.add_argument(
        "--automated",
        action="store_true",
        help="Bots: load credentials.env, skip prompts, register the RTCE MCP server with Claude Code",
    )
    parser.add_argument(
        "--testing",
        action="store_true",
        help="Bots: load credentials.env, skip prompts, no MCP registration",
    )
    args = parser.parse_args()
    if args.automated and args.testing:
        parser.error("--automated and --testing are mutually exclusive")

    print("=== Streamhouse Airline Demo — Deploy ===\n")
    root = get_project_root()
    print(f"Project root: {root}")

    # ---- Non-interactive -------------------------------------------------
    if args.automated or args.testing:
        creds_file = root / "credentials.env"
        if not creds_file.exists():
            print("Error: credentials.env not found. Run `uv run deploy` interactively first.")
            sys.exit(1)
        creds = dotenv_values(str(creds_file))

        missing = [
            label
            for key, label in {
                "TF_VAR_confluent_cloud_api_key": "Confluent Cloud API Key",
                "TF_VAR_confluent_cloud_api_secret": "Confluent Cloud API Secret",
            }.items()
            if not (creds.get(key) or "").strip()
        ]
        if missing:
            print("Error: credentials.env is missing:")
            for m in missing:
                print(f"  - {m}")
            sys.exit(1)

        region = creds.get("TF_VAR_cloud_region", DEFAULT_REGION) or DEFAULT_REGION
        prefix = creds.get("TF_VAR_resource_prefix", "") or ""
        ensure_confluent_login(creds)
        print(f"✓ Credentials loaded. Region: {region}. Deploying: {', '.join(DEPLOY_TARGETS)}\n")
        write_tfvars_for_deployment(root, region, creds, resource_prefix=prefix)
        for key, value in creds.items():
            if value:
                os.environ[key] = value
        _deploy(root, DEPLOY_TARGETS)
        _finish(root, "claude" if args.automated else "none")
        return

    # ---- Interactive -----------------------------------------------------
    creds_file, creds = load_or_create_credentials_file(root)

    # Confluent Cloud login (saved for auto-login).
    if not (creds.get("CONFLUENT_EMAIL") and creds.get("CONFLUENT_PASSWORD")):
        print("\nConfluent Cloud login (saved to credentials.env for auto-login):")
        for attempt in range(3):
            email = input("  Email (Enter to skip): ").strip()
            if not email:
                print("  Skipped. Run `confluent login` manually if your session expires.")
                break
            password = getpass.getpass("  Password: ")
            if _attempt_login_quiet(email, password):
                _save_env_safe(creds_file, "CONFLUENT_EMAIL", email)
                _save_env_safe(creds_file, "CONFLUENT_PASSWORD", password)
                creds["CONFLUENT_EMAIL"] = email
                creds["CONFLUENT_PASSWORD"] = password
                print("  ✓ Logged in and saved.")
                break
            remaining = 2 - attempt
            print(f"  Login failed. {remaining} attempt(s) left." if remaining else
                  "  Login failed. Skipping (use `confluent login --sso` if you use SSO).")
    ensure_confluent_login(creds)
    print("✓ Confluent CLI logged in")

    region = DEFAULT_REGION
    print(f"\nRegion: {region} (AWS-only; RTCE + Bedrock supported here)")

    # No prompt: reuse the saved prefix, else the default. Persisting it keeps
    # resource names stable if DEFAULT_PREFIX changes later.
    prefix = creds.get("TF_VAR_resource_prefix", "") or DEFAULT_PREFIX
    _save_env_safe(creds_file, "TF_VAR_resource_prefix", prefix)
    print(f"Resource name prefix: {prefix} (set TF_VAR_resource_prefix in credentials.env to change)")

    # Optionally generate fresh Confluent Cloud API keys.
    if input("\nGenerate new Confluent Cloud API keys? (y/n): ").strip().lower() == "y":
        api_key, api_secret = generate_confluent_api_keys(prefix)
        if api_key and api_secret:
            _save_env_safe(creds_file, "TF_VAR_confluent_cloud_api_key", api_key)
            _save_env_safe(creds_file, "TF_VAR_confluent_cloud_api_secret", api_secret)
            creds["TF_VAR_confluent_cloud_api_key"] = api_key
            creds["TF_VAR_confluent_cloud_api_secret"] = api_secret

    print("\n--- Credential Configuration ---")
    api_key = prompt_with_default(
        "Confluent Cloud API Key", creds.get("TF_VAR_confluent_cloud_api_key", "")
    )
    api_secret = prompt_with_default(
        "Confluent Cloud API Secret", creds.get("TF_VAR_confluent_cloud_api_secret", ""), secret=True
    )
    _save_env_safe(creds_file, "TF_VAR_confluent_cloud_api_key", api_key)
    _save_env_safe(creds_file, "TF_VAR_confluent_cloud_api_secret", api_secret)

    # AWS credentials power the recovery streaming agent (Bedrock) and the
    # keynote Tableflow S3/Glue analytics path (Demo 3) — both OPTIONAL, and
    # both use the same identity in a demo, so one credential covers both.
    # Try the ambient AWS credential chain first: pasting a long temporary
    # session token into a plain terminal prompt can freeze some terminals,
    # and most people running this already have valid AWS creds in their
    # shell (`aws sso login`, env vars, a profile).
    print(
        "\nAWS credentials power the recovery streaming agent (Bedrock) and the"
        "\nkeynote Tableflow S3/Glue analytics path (Demo 3). Both are optional —"
        "\nwithout them, everything else still deploys and those two are skipped."
    )
    access_key = secret_key = token = ""
    ambient = _detect_ambient_aws_credentials()
    if ambient:
        detected_key, detected_secret, detected_token = ambient
        masked = f"{detected_key[:4]}...{detected_key[-4:]}" if len(detected_key) > 8 else detected_key
        if input(f"Detected AWS credentials in your environment ({masked}). Use these? (Y/n): ").strip().lower() in (
            "",
            "y",
        ):
            access_key, secret_key, token = detected_key, detected_secret, detected_token

    if not access_key:
        access_key = prompt_with_default(
            "AWS Access Key (optional)",
            creds.get("TF_VAR_aws_bedrock_access_key", ""),
            required=False,
        )
        if access_key:
            secret_key = prompt_with_default(
                "AWS Secret Key", creds.get("TF_VAR_aws_bedrock_secret_key", ""), secret=True
            )
            if access_key.startswith("ASIA"):
                print(
                    "Temporary credentials need a session token too — that's a long string"
                    "\n(300-400 chars) that can freeze some terminals on paste. If it does,"
                    "\nCtrl-C and re-run after `export AWS_SESSION_TOKEN=...` (and"
                    "\nAWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY) in your shell, so this can"
                    "\nauto-detect it instead of needing it pasted here."
                )
                token = prompt_with_default(
                    "AWS Session Token (temp creds)", creds.get("TF_VAR_aws_session_token", ""), secret=True
                )

    if access_key and secret_key:
        for name in ("aws_bedrock_access_key", "aws_tableflow_access_key"):
            _save_env_safe(creds_file, f"TF_VAR_{name}", access_key)
        for name in ("aws_bedrock_secret_key", "aws_tableflow_secret_key"):
            _save_env_safe(creds_file, f"TF_VAR_{name}", secret_key)
        if token:
            for name in ("aws_session_token", "aws_tableflow_session_token"):
                _save_env_safe(creds_file, f"TF_VAR_{name}", token)

    _save_env_safe(creds_file, "TF_VAR_cloud_region", region)
    _flush_pending_writes(creds_file)

    final_creds = dotenv_values(creds_file)
    print("\n--- Configuration Summary ---")
    print(f"Resource name prefix: {prefix}")
    print(f"Region: {region}")
    api_key_state = "set" if final_creds.get("TF_VAR_confluent_cloud_api_key") else "MISSING"
    print(f"Confluent Cloud API key: {api_key_state}")
    print(f"Confluent CLI auto-login: {'saved' if final_creds.get('CONFLUENT_EMAIL') else 'not saved'}")
    agent_state = "ENABLED" if final_creds.get("TF_VAR_aws_bedrock_access_key") else "skipped (no Bedrock creds)"
    print(f"Streaming agent: {agent_state}")
    analytics_state = (
        "ENABLED" if final_creds.get("TF_VAR_aws_tableflow_access_key") else "skipped (no Tableflow AWS creds)"
    )
    print(f"Keynote S3/Glue analytics (Demo 3): {analytics_state}")
    print(f"Deploying: {', '.join(DEPLOY_TARGETS)}, then demo data and RTCE")

    client = setup_rtce._pick_client()

    if input("\nReady to deploy? (y/n): ").strip().lower() != "y":
        print("Deployment cancelled.")
        sys.exit(0)

    write_tfvars_for_deployment(root, region, final_creds, resource_prefix=prefix)
    for key, value in final_creds.items():
        if value:
            os.environ[key] = value
    _deploy(root, DEPLOY_TARGETS)
    _finish(root, client)
    if input("\nStart the app at http://127.0.0.1:8000 now? (Y/n): ").strip().lower() in ("", "y"):
        from scripts.keynote_app import main as run_app

        run_app([])
    else:
        print("Start it later with: uv run airport-app")


def _finish(root, client: str) -> None:
    """Publish the demo data, then enable Lightning Tables/RTCE on the demo topics."""
    print("\n=== Publishing demo data ===")
    _run_keynote_datagen()
    print("\n=== Enabling Lightning Tables and RTCE ===")
    setup_rtce.main(["--client", client])
    _print_env_name(root)


def _run_keynote_datagen() -> None:
    """Use the same sequence as `uv run airport-datagen` after provisioning."""
    import time

    from confluent_kafka.schema_registry import SchemaRegistryClient

    from .keynote_datagen import _clock, run
    from .terraform import extract_kafka_credentials

    credentials = extract_kafka_credentials("aws", get_project_root())
    registry = SchemaRegistryClient({
        "url": credentials["schema_registry_url"],
        "basic.auth.user.info": (
            f"{credentials['schema_registry_api_key']}:{credentials['schema_registry_api_secret']}")})
    subjects = ("flight_status-value", "passenger_connections-value",
                "hotel_inventory-value", "passenger_recommendations-value")
    for attempt in range(30):
        try:
            for subject in subjects:
                registry.get_latest_version(subject)
            break
        except Exception:
            if attempt == 29:
                raise RuntimeError("Keynote schemas did not become ready after deployment") from None
            time.sleep(2)
    run(_clock(None), seed=42, phases=["seed", "delay", "offers", "sellout"],
        dry_run=False, pause=15)


def _print_env_name(root) -> None:
    core_state = root / "terraform" / "core" / "terraform.tfstate"
    if not core_state.exists():
        return
    try:
        outputs = run_terraform_output(core_state)
        if "confluent_environment_display_name" in outputs:
            print(f"\nEnvironment name: {outputs['confluent_environment_display_name']}")
    except Exception as e:
        print(f"\n⚠ Could not read Terraform outputs: {e}")


if __name__ == "__main__":
    main()

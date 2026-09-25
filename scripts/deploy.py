#!/usr/bin/env python3
"""
Deploy the Streamhouse airline flight-recovery demo to Confluent Cloud.

Always deploys, in order:
  1. terraform/core        — environment, cluster, Flink pool, service accounts,
                             API keys, RTCE reader, (optional) Bedrock connection.
  2. terraform/airline-demo — the airline Flink pipeline (tables, maintained state,
                             ops spoke, and the recovery streaming agent when
                             Bedrock creds are present).

AWS-only by design (RTCE + Flink Native Inference are AWS-only), so there is no
cloud or region choice. Resource names use TF_VAR_resource_prefix from
credentials.env, or DEFAULT_PREFIX when unset (saved on first run so later
runs keep the same names).

Usage:
  uv run deploy               # interactive
  uv run deploy --automated   # non-interactive from credentials.env, then setup-mcp
  uv run deploy --testing     # non-interactive from credentials.env, no setup-mcp
"""

import argparse
import getpass
import os
import sys
import time

from dotenv import dotenv_values, set_key

from scripts.credentials import (
    generate_confluent_api_keys,
    load_or_create_credentials_file,
)
from scripts.login_checks import _attempt_login_quiet, ensure_confluent_login
from scripts.mcp_setup import main as setup_mcp
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
        "--with-datagen", action="store_true",
        help="Run the finite keynote fixture after both Terraform roots finish",
    )
    parser.add_argument(
        "--automated",
        action="store_true",
        help="Non-interactive: load credentials.env, skip prompts, run setup-mcp after deploy",
    )
    parser.add_argument(
        "--testing",
        action="store_true",
        help="Non-interactive: load credentials.env, skip prompts, no setup-mcp",
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
        if args.with_datagen:
            _run_keynote_datagen()
        if args.automated:
            setup_mcp()
        _print_env_name(root)
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

    # AWS Bedrock creds are OPTIONAL — without them the recovery streaming agent
    # is skipped and everything else still deploys.
    print(
        "\nAWS Bedrock credentials power the recovery streaming agent."
        "\nOptional: with none saved, leave blank to deploy everything except the agent"
        "\nand add it later."
    )
    bedrock_key = prompt_with_default(
        "AWS Bedrock Access Key (optional)",
        creds.get("TF_VAR_aws_bedrock_access_key", ""),
        required=False,
    )
    if bedrock_key:
        bedrock_secret = prompt_with_default(
            "AWS Bedrock Secret Key", creds.get("TF_VAR_aws_bedrock_secret_key", ""), secret=True
        )
        _save_env_safe(creds_file, "TF_VAR_aws_bedrock_access_key", bedrock_key)
        _save_env_safe(creds_file, "TF_VAR_aws_bedrock_secret_key", bedrock_secret)
        if bedrock_key.startswith("ASIA"):
            token = prompt_with_default(
                "AWS Session Token (temp creds)", creds.get("TF_VAR_aws_session_token", ""), secret=True
            )
            if token:
                _save_env_safe(creds_file, "TF_VAR_aws_session_token", token)

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
    print(f"Deploying: {', '.join(DEPLOY_TARGETS)}")

    if input("\nReady to deploy? (y/n): ").strip().lower() != "y":
        print("Deployment cancelled.")
        sys.exit(0)

    write_tfvars_for_deployment(root, region, final_creds, resource_prefix=prefix)
    for key, value in final_creds.items():
        if value:
            os.environ[key] = value
    _deploy(root, DEPLOY_TARGETS)
    if args.with_datagen:
        _run_keynote_datagen()
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

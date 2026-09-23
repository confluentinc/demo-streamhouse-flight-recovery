"""
Terraform variables file (terraform.tfvars) management utilities.

Provides functions for:
- Writing terraform.tfvars files with automatic backup
- Generating formatted tfvars content for the core module
"""

import shutil
from pathlib import Path


def get_credential_value(creds: dict[str, str], key: str) -> str | None:
    """
    Get credential value, checking both TF_VAR_ prefixed and non-prefixed keys.

    Args:
        creds: Dictionary of credentials
        key: Key to look up (without TF_VAR_ prefix)

    Returns:
        Value if found, None otherwise
    """
    return creds.get(key) or creds.get(f"TF_VAR_{key}")


def write_tfvars_file(tfvars_path: Path, content: str) -> bool:
    """
    Write terraform.tfvars file with backup of existing file.

    Args:
        tfvars_path: Path to terraform.tfvars file
        content: Content to write

    Returns:
        True if successful, False otherwise
    """
    try:
        # Backup existing file
        if tfvars_path.exists():
            backup_path = tfvars_path.with_suffix(".tfvars.backup")
            shutil.copy2(tfvars_path, backup_path)

        # Ensure parent directory exists
        tfvars_path.parent.mkdir(parents=True, exist_ok=True)

        # Write new content
        with open(tfvars_path, "w") as f:
            f.write(content)

        return True
    except Exception as e:
        print(f"Error writing {tfvars_path}: {e}")
        return False


def generate_core_tfvars_content(
    region: str,
    api_key: str,
    api_secret: str,
    resource_prefix: str | None = None,
    aws_bedrock_access_key: str | None = None,
    aws_bedrock_secret_key: str | None = None,
    aws_session_token: str | None = None,
) -> str:
    """
    Generate terraform.tfvars content for the (AWS-only) core module.

    Bedrock credentials are optional: when omitted, the topic/serving/ops stack
    still deploys and the recovery streaming agent is skipped.

    Args:
        region: AWS region
        api_key: Confluent Cloud API key
        api_secret: Confluent Cloud API secret
        resource_prefix: Resource name prefix (defaults to the terraform default)
        aws_bedrock_access_key: AWS Bedrock access key (optional)
        aws_bedrock_secret_key: AWS Bedrock secret key (optional)
        aws_session_token: AWS session token (optional, for ASIA* temp creds)

    Returns:
        Formatted terraform.tfvars content
    """
    content = f"""# Core Infrastructure Configuration (AWS-only)
cloud_region = "{region}"
confluent_cloud_api_key = "{api_key}"
confluent_cloud_api_secret = "{api_secret}"
"""
    if resource_prefix:
        content += f'resource_prefix = "{resource_prefix}"\n'

    if aws_bedrock_access_key and aws_bedrock_secret_key:
        content += f'aws_bedrock_access_key = "{aws_bedrock_access_key}"\n'
        content += f'aws_bedrock_secret_key = "{aws_bedrock_secret_key}"\n'
        if aws_session_token:
            content += f'aws_session_token = "{aws_session_token}"\n'

    return content


def write_tfvars_for_deployment(
    root: Path, region: str, creds: dict[str, str], resource_prefix: str = ""
) -> None:
    """
    Write terraform.tfvars for the core module (AWS-only, airline demo).

    airline-demo needs no tfvars of its own — it inherits everything from core via
    terraform_remote_state, and enable_streaming_agent defaults to true (auto-skipped
    when core has no Bedrock connection).

    Args:
        root: Project root directory
        region: AWS region
        creds: Credentials dict (supports TF_VAR_-prefixed and bare keys)
        resource_prefix: Optional resource name prefix override
    """
    api_key = get_credential_value(creds, "confluent_cloud_api_key")
    api_secret = get_credential_value(creds, "confluent_cloud_api_secret")
    if not (api_key and api_secret):
        return

    core_tfvars_path = root / "terraform" / "core" / "terraform.tfvars"
    content = generate_core_tfvars_content(
        region,
        api_key,
        api_secret,
        resource_prefix=resource_prefix or None,
        aws_bedrock_access_key=get_credential_value(creds, "aws_bedrock_access_key"),
        aws_bedrock_secret_key=get_credential_value(creds, "aws_bedrock_secret_key"),
        aws_session_token=get_credential_value(creds, "aws_session_token"),
    )
    if write_tfvars_file(core_tfvars_path, content):
        print(f"✓ Wrote {core_tfvars_path}")

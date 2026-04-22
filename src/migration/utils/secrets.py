"""AWS Secrets Manager helper with process-local caching.

Secrets Manager charges per API call. For long-running Glue jobs that call
`get_secret` dozens of times per batch, caching is essential. We use
`@lru_cache` for process-lifetime caching.

Cache invalidation: secrets rotated after a process starts are NOT picked up
until the process restarts. This is acceptable for batch jobs (typically
<1 hour). For long-running services, use a TTL-based cache instead.
"""

from __future__ import annotations

import json
from functools import lru_cache
from typing import Any

import boto3

from migration.utils.logging_config import get_logger

log = get_logger(__name__, component="secrets")


@lru_cache(maxsize=32)
def get_secret(secret_id: str, region: str = "us-east-1") -> dict[str, Any]:
    """Fetch a secret from AWS Secrets Manager.

    Expects the secret to be a JSON string. Returns the parsed dict.

    :param secret_id: secret name or ARN
    :param region: AWS region
    """
    client = boto3.client("secretsmanager", region_name=region)
    try:
        response = client.get_secret_value(SecretId=secret_id)
    except client.exceptions.ResourceNotFoundException as exc:
        log.error("secret_not_found", secret_id=secret_id, region=region)
        raise RuntimeError(f"Secret not found: {secret_id}") from exc

    secret_string = response.get("SecretString")
    if not secret_string:
        raise RuntimeError(f"Secret {secret_id} has no SecretString value")

    try:
        parsed = json.loads(secret_string)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Secret {secret_id} is not valid JSON — use put-secret-value "
            f"with a JSON-structured value"
        ) from exc

    log.debug("secret_loaded", secret_id=secret_id)
    return parsed


def invalidate_cache() -> None:
    """Clear the secrets cache. Call after rotation in long-running processes."""
    get_secret.cache_clear()

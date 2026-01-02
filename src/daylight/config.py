from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import os


class ConfigError(ValueError):
    pass


def _get_env(name: str, *, default: str | None = None, required: bool = False) -> str | None:
    value = os.getenv(name, default)
    if required and (value is None or value.strip() == ""):
        raise ConfigError(f"Missing required environment variable: {name}")
    return value


def _parse_int(name: str, value: str | None, *, minimum: int | None = None,
               maximum: int | None = None) -> int:
    if value is None:
        raise ConfigError(f"Missing required integer environment variable: {name}")
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ConfigError(f"Invalid integer for {name}: {value}") from exc
    if minimum is not None and parsed < minimum:
        raise ConfigError(f"{name} must be >= {minimum}")
    if maximum is not None and parsed > maximum:
        raise ConfigError(f"{name} must be <= {maximum}")
    return parsed


def _parse_bool(name: str, value: str | None, *, default: bool | None = None) -> bool:
    if value is None:
        if default is None:
            raise ConfigError(f"Missing required boolean environment variable: {name}")
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    raise ConfigError(f"Invalid boolean for {name}: {value}")


def _parse_optional_datetime(name: str, value: str | None) -> datetime | None:
    if value is None or value.strip() == "":
        return None
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        try:
            parsed = datetime.strptime(normalized, "%Y-%m-%d %I:%M:%S %p")
        except ValueError as exc:
            raise ConfigError(f"Invalid datetime for {name}: {value}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


@dataclass(frozen=True)
class Config:
    cloudflare_api_token: str
    cloudflare_account_id: str
    cloudflare_per_page: int
    cloudflare_direction: str
    cloudflare_hide_user_logs: bool
    cloudflare_since_safety_lag_seconds: int
    cloudflare_request_timeout_seconds: int
    poll_interval_seconds: int
    initial_checkpoint: datetime | None
    pg_host: str
    pg_port: int
    pg_database: str
    pg_user: str
    pg_password: str
    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str
    s3_bucket: str
    s3_region: str


def load_config() -> Config:
    cloudflare_api_token = _get_env("CLOUDFLARE_API_TOKEN", required=True)
    cloudflare_account_id = _get_env("CLOUDFLARE_ACCOUNT_ID", required=True)
    cloudflare_per_page = _parse_int(
        "CLOUDFLARE_PER_PAGE",
        _get_env("CLOUDFLARE_PER_PAGE", default="1000"),
        minimum=1,
        maximum=1000,
    )
    cloudflare_direction = _get_env("CLOUDFLARE_DIRECTION", default="asc")
    if cloudflare_direction not in {"asc", "desc"}:
        raise ConfigError("CLOUDFLARE_DIRECTION must be 'asc' or 'desc'")
    cloudflare_hide_user_logs = _parse_bool(
        "CLOUDFLARE_HIDE_USER_LOGS",
        _get_env("CLOUDFLARE_HIDE_USER_LOGS", default="false"),
    )
    cloudflare_since_safety_lag_seconds = _parse_int(
        "CLOUDFLARE_SINCE_SAFETY_LAG_SECONDS",
        _get_env("CLOUDFLARE_SINCE_SAFETY_LAG_SECONDS", default="60"),
        minimum=0,
    )
    cloudflare_request_timeout_seconds = _parse_int(
        "CLOUDFLARE_REQUEST_TIMEOUT_SECONDS",
        _get_env("CLOUDFLARE_REQUEST_TIMEOUT_SECONDS", default="30"),
        minimum=1,
    )
    poll_interval_seconds = _parse_int(
        "POLL_INTERVAL_SECONDS",
        _get_env("POLL_INTERVAL_SECONDS", default="60"),
        minimum=1,
    )
    initial_checkpoint = _parse_optional_datetime(
        "INITIAL_CHECKPOINT",
        _get_env("INITIAL_CHECKPOINT"),
    )

    pg_host = _get_env("PG_HOST", required=True)
    pg_port = _parse_int("PG_PORT", _get_env("PG_PORT", required=True), minimum=1)
    pg_database = _get_env("PG_DATABASE", required=True)
    pg_user = _get_env("PG_USER", required=True)
    pg_password = _get_env("PG_PASSWORD", required=True)

    s3_endpoint = _get_env("S3_ENDPOINT", required=True)
    s3_access_key = _get_env("S3_ACCESS_KEY", required=True)
    s3_secret_key = _get_env("S3_SECRET_KEY", required=True)
    s3_bucket = _get_env("S3_BUCKET", required=True)
    s3_region = _get_env("S3_REGION", required=True)

    return Config(
        cloudflare_api_token=cloudflare_api_token,
        cloudflare_account_id=cloudflare_account_id,
        cloudflare_per_page=cloudflare_per_page,
        cloudflare_direction=cloudflare_direction,
        cloudflare_hide_user_logs=cloudflare_hide_user_logs,
        cloudflare_since_safety_lag_seconds=cloudflare_since_safety_lag_seconds,
        cloudflare_request_timeout_seconds=cloudflare_request_timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
        initial_checkpoint=initial_checkpoint,
        pg_host=pg_host,
        pg_port=pg_port,
        pg_database=pg_database,
        pg_user=pg_user,
        pg_password=pg_password,
        s3_endpoint=s3_endpoint,
        s3_access_key=s3_access_key,
        s3_secret_key=s3_secret_key,
        s3_bucket=s3_bucket,
        s3_region=s3_region,
    )

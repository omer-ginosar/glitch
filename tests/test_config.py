from datetime import timezone

import pytest

from daylight.config import ConfigError, load_config


def _set_required_env(monkeypatch):
    monkeypatch.setenv("CLOUDFLARE_API_TOKEN", "token")
    monkeypatch.setenv("CLOUDFLARE_ACCOUNT_ID", "account")
    monkeypatch.setenv("PG_HOST", "localhost")
    monkeypatch.setenv("PG_PORT", "5432")
    monkeypatch.setenv("PG_DATABASE", "audit_logs")
    monkeypatch.setenv("PG_USER", "postgres")
    monkeypatch.setenv("PG_PASSWORD", "postgres")
    monkeypatch.setenv("S3_ENDPOINT", "http://localhost:9000")
    monkeypatch.setenv("S3_ACCESS_KEY", "minio")
    monkeypatch.setenv("S3_SECRET_KEY", "minio")
    monkeypatch.setenv("S3_BUCKET", "audit-logs")
    monkeypatch.setenv("S3_REGION", "us-east-1")
    monkeypatch.setenv("S3_PREFIX", "dev/audit-logs")


def test_load_config_success(monkeypatch):
    _set_required_env(monkeypatch)
    monkeypatch.setenv("POLL_INTERVAL_SECONDS", "10")
    monkeypatch.setenv("CLOUDFLARE_PER_PAGE", "500")
    monkeypatch.setenv("CLOUDFLARE_DIRECTION", "asc")
    config = load_config()
    assert config.cloudflare_per_page == 500
    assert config.poll_interval_seconds == 10
    assert config.s3_prefix == "dev/audit-logs"


def test_load_config_rejects_invalid_direction(monkeypatch):
    _set_required_env(monkeypatch)
    monkeypatch.setenv("CLOUDFLARE_DIRECTION", "sideways")
    with pytest.raises(ConfigError):
        load_config()


def test_initial_checkpoint_parsing(monkeypatch):
    _set_required_env(monkeypatch)
    monkeypatch.setenv("INITIAL_CHECKPOINT", "2026-01-02 01:09:27 PM")
    config = load_config()
    assert config.initial_checkpoint is not None
    assert config.initial_checkpoint.tzinfo == timezone.utc
    assert config.initial_checkpoint.hour == 13

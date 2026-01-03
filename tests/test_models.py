from datetime import datetime, timezone

import pytest

from daylight.models import AuditLogRecord, SchemaError


def test_audit_log_mapping():
    event = {
        "id": "evt-1",
        "when": "2026-01-02T15:37:23Z",
        "action": {"type": "token_update", "result": True},
        "actor": {"email": "user@example.com", "type": "user", "ip": "127.0.0.1"},
        "resource": {"type": "account", "id": "acct-1"},
        "metadata": {"token_name": "Read analytics"},
    }
    record = AuditLogRecord.from_cloudflare_event(
        event,
        account_id="acct-1",
        ingestion_time=datetime.now(timezone.utc),
    )
    assert record.event_id == "evt-1"
    assert record.timestamp.tzinfo == timezone.utc
    assert record.action_type == "token_update"
    assert record.action_result == "true"
    assert record.metadata == {"token_name": "Read analytics"}


def test_missing_id_rejected():
    event = {"when": "2026-01-02T15:37:23Z"}
    with pytest.raises(SchemaError):
        AuditLogRecord.from_cloudflare_event(
            event,
            account_id="acct-1",
            ingestion_time=datetime.now(timezone.utc),
        )

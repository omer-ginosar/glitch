from datetime import datetime, timezone
from unittest.mock import Mock

from daylight.db import insert_audit_logs, load_checkpoint, save_checkpoint
from daylight.models import AuditLogRecord


class _Cursor:
    def __init__(self):
        self.executed = []
        self.rowcount = 2
        self._row = None

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        return self._row

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _Conn:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


def _make_record():
    return AuditLogRecord(
        event_id="evt-1",
        timestamp=datetime(2026, 1, 2, tzinfo=timezone.utc),
        account_id="acct",
        actor_email="user@example.com",
        actor_type="user",
        action_type="token_update",
        action_result="true",
        resource_type="account",
        resource_id="acct",
        ip_address="127.0.0.1",
        metadata={"token_name": "Read analytics"},
        ingestion_time=datetime(2026, 1, 2, tzinfo=timezone.utc),
        raw_event={"id": "evt-1"},
    )


def test_insert_audit_logs_calls_execute_values(monkeypatch):
    cursor = _Cursor()
    conn = _Conn(cursor)
    called = {}

    def execute_values_stub(cur, sql, values, page_size=500):
        called["sql"] = sql
        called["values"] = list(values)

    monkeypatch.setattr("daylight.db.execute_values", execute_values_stub)

    inserted = insert_audit_logs(conn, [_make_record()])
    assert inserted == cursor.rowcount
    assert "INSERT INTO audit_logs" in called["sql"]
    assert len(called["values"]) == 1


def test_insert_audit_logs_empty(monkeypatch):
    cursor = _Cursor()
    conn = _Conn(cursor)
    execute_values_stub = Mock()
    monkeypatch.setattr("daylight.db.execute_values", execute_values_stub)
    assert insert_audit_logs(conn, []) == 0
    execute_values_stub.assert_not_called()


def test_checkpoint_helpers():
    cursor = _Cursor()
    conn = _Conn(cursor)
    save_checkpoint(conn, datetime(2026, 1, 2, tzinfo=timezone.utc))
    assert any("ingestion_checkpoint" in sql for sql, _ in cursor.executed)

    cursor._row = (datetime(2026, 1, 2, tzinfo=timezone.utc),)
    assert load_checkpoint(conn) is not None

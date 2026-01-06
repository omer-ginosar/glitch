"""Rehydrate audit logs from object storage into the database."""

from __future__ import annotations

import argparse
from collections.abc import Mapping
from datetime import datetime, timezone
import json
from typing import Any

from glitch.config import ConfigError, load_config
from glitch.db import (
    connect,
    ensure_audit_logs_view,
    ensure_rehydrated_audit_logs_table,
    insert_rehydrated_audit_logs,
)
from glitch.models import AuditLogRecord
from glitch.object_storage import ObjectStorageClient, ObjectStorageError


BATCH_SIZE = 500


def _parse_rfc3339(value: str) -> datetime:
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise SystemExit(f"Invalid RFC3339 datetime: {value}") from exc
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _load_json(value: Any) -> Mapping[str, Any]:
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return value
    if isinstance(value, str):
        if not value:
            return {}
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, Mapping):
            return parsed
    return {}


def _normalize_timestamp(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _record_from_row(
    row: Mapping[str, Any],
    *,
    since: datetime,
    before: datetime,
    default_account_id: str,
    expected_account_id: str | None = None,
) -> AuditLogRecord | None:
    event_id = row.get("event_id")
    if not event_id:
        return None
    timestamp = _normalize_timestamp(row.get("timestamp"))
    if timestamp is None or timestamp < since or timestamp >= before:
        return None
    ingestion_time = _normalize_timestamp(row.get("ingestion_time")) or timestamp
    account_id = row.get("account_id") or default_account_id
    if expected_account_id and account_id != expected_account_id:
        return None
    return AuditLogRecord(
        event_id=str(event_id),
        timestamp=timestamp,
        account_id=account_id,
        actor_email=row.get("actor_email"),
        actor_type=row.get("actor_type"),
        action_type=row.get("action_type"),
        action_result=row.get("action_result"),
        resource_type=row.get("resource_type"),
        resource_id=row.get("resource_id"),
        ip_address=row.get("ip_address"),
        metadata=_load_json(row.get("metadata_json")),
        ingestion_time=ingestion_time,
        raw_event=_load_json(row.get("raw_event_json")),
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Rehydrate audit logs from object storage."
    )
    parser.add_argument("--since", required=True, help="RFC3339 start time")
    parser.add_argument("--before", required=True, help="RFC3339 end time")
    parser.add_argument("--account-id", help="Cloudflare account ID")
    args = parser.parse_args()

    since = _parse_rfc3339(args.since)
    before = _parse_rfc3339(args.before)
    if since >= before:
        raise SystemExit("--since must be earlier than --before")

    try:
        config = load_config()
    except ConfigError as exc:
        raise SystemExit(str(exc)) from exc
    account_id = args.account_id or config.cloudflare_account_id

    object_storage = ObjectStorageClient.from_config(config)
    conn = connect(config)
    with conn:
        ensure_rehydrated_audit_logs_table(conn)
        ensure_audit_logs_view(conn)

    try:
        keys = object_storage.list_keys_for_window(
            since=since,
            before=before,
            account_id=account_id,
        )
    except Exception as exc:
        raise SystemExit(str(exc)) from exc

    batch: list[AuditLogRecord] = []
    for key in keys:
        try:
            rows = object_storage.read_parquet_records(key)
        except ObjectStorageError as exc:
            raise SystemExit(str(exc)) from exc
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            record = _record_from_row(
                row,
                since=since,
                before=before,
                default_account_id=account_id,
                expected_account_id=account_id,
            )
            if record is None:
                continue
            batch.append(record)
            if len(batch) >= BATCH_SIZE:
                with conn:
                    insert_rehydrated_audit_logs(conn, batch)
                batch.clear()

    if batch:
        with conn:
            insert_rehydrated_audit_logs(conn, batch)


if __name__ == "__main__":
    main()

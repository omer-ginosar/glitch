from __future__ import annotations

import os
import re
import sys
from datetime import datetime, timedelta, timezone

import pytest

from glitch.config import load_config
from glitch.db import (
    connect,
    ensure_audit_logs_view,
    ensure_rehydrated_audit_logs_table,
)
from glitch.object_storage import ObjectStorageClient
from glitch import rehydrate


def _parse_rfc3339(value: str) -> datetime:
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _bool_env(name: str, *, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _load_dotenv(path: str) -> None:
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            if stripped.startswith("export "):
                stripped = stripped[len("export ") :].lstrip()
            if "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            key = key.strip()
            value = value.strip()
            if not key or key in os.environ:
                continue
            if value and value[0] == value[-1] and value[0] in {"'", '"'}:
                value = value[1:-1]
            os.environ[key] = value


def _load_dotenv_if_present() -> None:
    root = os.path.abspath(os.getcwd())
    _load_dotenv(os.path.join(root, ".env"))


_HOUR_RE = re.compile(r"year=(\d{4})/month=(\d{2})/day=(\d{2})/hour=(\d{2})")


def _extract_hour_from_key(key: str) -> datetime | None:
    match = _HOUR_RE.search(key)
    if not match:
        return None
    year, month, day, hour = (int(value) for value in match.groups())
    return datetime(year, month, day, hour, tzinfo=timezone.utc)


def _list_all_keys(client: ObjectStorageClient) -> list[str]:
    prefix = client._prefix.strip("/")  # pylint: disable=protected-access
    if prefix:
        prefix = f"{prefix}/"
    continuation: str | None = None
    keys: list[str] = []
    while True:
        params = {"Bucket": client._bucket, "Prefix": prefix}  # pylint: disable=protected-access
        if continuation:
            params["ContinuationToken"] = continuation
        response = client._s3_client.list_objects_v2(**params)  # pylint: disable=protected-access
        for entry in response.get("Contents", []):
            key = entry.get("Key")
            if key:
                keys.append(key)
        if not response.get("IsTruncated"):
            break
        continuation = response.get("NextContinuationToken")
    return keys


def _default_window_from_storage(
    client: ObjectStorageClient,
    *,
    hours: int,
) -> tuple[str, str]:
    keys = _list_all_keys(client)
    latest: datetime | None = None
    for key in keys:
        hour = _extract_hour_from_key(key)
        if hour and (latest is None or hour > latest):
            latest = hour
    if latest is None:
        pytest.fail("No object storage keys found to derive a default window")
    before = latest + timedelta(hours=1)
    since = before - timedelta(hours=hours)
    before_raw = before.isoformat().replace("+00:00", "Z")
    since_raw = since.isoformat().replace("+00:00", "Z")
    return since_raw, before_raw


def _default_window_from_db(conn, *, hours: int) -> tuple[str, str]:
    with conn.cursor() as cursor:
        cursor.execute("SELECT MAX(timestamp) FROM audit_logs")
        row = cursor.fetchone()
    latest = row[0] if row else None
    if latest is None:
        pytest.fail("No audit_logs rows found to derive a default window")
    before = latest + timedelta(hours=1)
    since = before - timedelta(hours=hours)
    before_raw = before.isoformat().replace("+00:00", "Z")
    since_raw = since.isoformat().replace("+00:00", "Z")
    return since_raw, before_raw


@pytest.mark.integration
def test_rehydrate_no_duplicates_between_tables():
    _load_dotenv_if_present()
    since_raw = os.getenv("REHYDRATE_SINCE")
    before_raw = os.getenv("REHYDRATE_BEFORE")
    if not _bool_env("REHYDRATE_ALLOW_MUTATION", default=True):
        pytest.fail("REHYDRATE_ALLOW_MUTATION=false; test mutates audit_logs")

    injected_token = False
    if not os.getenv("CLOUDFLARE_ACCOUNT_ID"):
        pytest.fail("CLOUDFLARE_ACCOUNT_ID must be set for rehydration")
    if not os.getenv("CLOUDFLARE_API_TOKEN"):
        os.environ["CLOUDFLARE_API_TOKEN"] = "rehydrate-test-token"
        injected_token = True

    try:
        config = load_config()
    finally:
        if injected_token:
            os.environ.pop("CLOUDFLARE_API_TOKEN", None)
    account_id = config.cloudflare_account_id
    conn = connect(config)
    with conn:
        ensure_rehydrated_audit_logs_table(conn)
        ensure_audit_logs_view(conn)

    if not since_raw or not before_raw:
        since_raw, before_raw = _default_window_from_db(conn, hours=72)
    object_storage = ObjectStorageClient.from_config(config)
    if not since_raw or not before_raw:
        since_raw, before_raw = _default_window_from_storage(
            object_storage, hours=72
        )
    since = _parse_rfc3339(since_raw)
    before = _parse_rfc3339(before_raw)
    if since >= before:
        pytest.fail("Invalid REHYDRATE_SINCE/REHYDRATE_BEFORE window")
    keys = object_storage.list_keys_for_window(
        since=since,
        before=before,
        account_id=account_id,
    )
    assert keys, "No object storage files found for the window"

    total_rows = 0
    for key in keys:
        rows = object_storage.read_parquet_records(key)
        for row in rows:
            ts = row.get("timestamp") if isinstance(row, dict) else None
            if isinstance(ts, datetime):
                ts = ts.astimezone(timezone.utc)
            if ts is not None and since <= ts < before:
                total_rows += 1
    assert total_rows >= 2, "Need at least 2 rows in object storage for the window"

    with conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT event_id, timestamp, account_id
                FROM audit_logs
                WHERE timestamp >= %s AND timestamp < %s
                ORDER BY timestamp ASC
                LIMIT 1
                """,
                (since, before),
            )
            keep = cursor.fetchone()
            if keep is None:
                pytest.fail("No audit_logs rows found in the window")
            keep_event_id, keep_timestamp, keep_account_id = keep

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM audit_logs
                WHERE timestamp >= %s AND timestamp < %s
                AND account_id = %s
                """,
                (since, before, keep_account_id),
            )
            original_count = cursor.fetchone()[0]
            assert original_count >= 2, "Need at least 2 rows in audit_logs to test rehydration"

            cursor.execute(
                """
                DELETE FROM audit_logs
                WHERE timestamp >= %s AND timestamp < %s
                AND account_id = %s
                AND NOT (timestamp = %s AND event_id = %s AND account_id = %s)
                """,
                (
                    since,
                    before,
                    keep_account_id,
                    keep_timestamp,
                    keep_event_id,
                    keep_account_id,
                ),
            )

    argv = sys.argv[:]
    sys.argv = [
        "glitch.rehydrate",
        "--since",
        since_raw,
        "--before",
        before_raw,
        "--account-id",
        account_id,
    ]
    try:
        rehydrate.main()
    finally:
        sys.argv = argv

    with conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM audit_logs
                WHERE timestamp = %s AND event_id = %s AND account_id = %s
                """,
                (keep_timestamp, keep_event_id, keep_account_id),
            )
            assert cursor.fetchone()[0] == 1

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM audit_logs_rehydrated
                WHERE timestamp = %s AND event_id = %s AND account_id = %s
                """,
                (keep_timestamp, keep_event_id, keep_account_id),
            )
            assert cursor.fetchone()[0] == 0

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM audit_logs a
                JOIN audit_logs_rehydrated r
                ON a.timestamp = r.timestamp
                AND a.event_id = r.event_id
                AND a.account_id = r.account_id
                WHERE a.timestamp >= %s AND a.timestamp < %s
                AND a.account_id = %s
                """,
                (since, before, keep_account_id),
            )
            assert cursor.fetchone()[0] == 0

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM audit_logs_rehydrated
                WHERE timestamp >= %s AND timestamp < %s
                AND account_id = %s
                """,
                (since, before, keep_account_id),
            )
            assert cursor.fetchone()[0] >= 1

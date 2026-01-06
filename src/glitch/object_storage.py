from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import logging
import os
import socket
from typing import Any, Mapping, Sequence
from urllib.parse import urlparse, urlunparse
from uuid import uuid4

import boto3
from botocore.exceptions import ClientError
import pyarrow as pa
import pyarrow.parquet as pq

from glitch.config import Config
from glitch.models import AuditLogRecord


logger = logging.getLogger(__name__)


class ObjectStorageError(RuntimeError):
    pass


PARQUET_COMPRESSION = "snappy"
PARQUET_SCHEMA = pa.schema(
    [
        ("event_id", pa.string()),
        ("timestamp", pa.timestamp("ns", tz="UTC")),
        ("account_id", pa.string()),
        ("actor_email", pa.string()),
        ("actor_type", pa.string()),
        ("action_type", pa.string()),
        ("action_result", pa.string()),
        ("resource_type", pa.string()),
        ("resource_id", pa.string()),
        ("ip_address", pa.string()),
        ("metadata_json", pa.string()),
        ("raw_event_json", pa.string()),
        ("ingestion_time", pa.timestamp("ns", tz="UTC")),
    ]
)


def _normalize_timestamp(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def partition_path(value: datetime, *, prefix: str = "") -> str:
    normalized = _normalize_timestamp(value)
    path = (
        f"year={normalized.year:04d}/"
        f"month={normalized.month:02d}/"
        f"day={normalized.day:02d}/"
        f"hour={normalized.hour:02d}"
    )
    if prefix:
        prefix = prefix.strip("/")
        return f"{prefix}/{path}"
    return path


def _serialize_json(value: Mapping[str, Any]) -> str:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        )
    except TypeError as exc:
        raise ObjectStorageError("Failed to serialize JSON payload") from exc


def _coerce_json_string(value: Any) -> str:
    if value is None:
        return "{}"
    if isinstance(value, str):
        return value
    if isinstance(value, Mapping):
        return _serialize_json(value)
    try:
        return json.dumps(value, ensure_ascii=True)
    except TypeError:
        return "{}"


def _resolve_s3_endpoint(endpoint: str) -> str:
    parsed = urlparse(endpoint)
    if not parsed.scheme or not parsed.netloc:
        return endpoint
    if parsed.hostname != "minio":
        return endpoint
    if os.path.exists("/.dockerenv"):
        return endpoint
    port = parsed.port or 9000
    try:
        socket.getaddrinfo(parsed.hostname, port, socket.AF_UNSPEC, socket.SOCK_STREAM)
        return endpoint
    except socket.gaierror:
        pass
    userinfo = ""
    if parsed.username:
        userinfo = parsed.username
        if parsed.password:
            userinfo += f":{parsed.password}"
        userinfo += "@"
    netloc = f"{userinfo}localhost"
    if parsed.port:
        netloc = f"{netloc}:{parsed.port}"
    return urlunparse(parsed._replace(netloc=netloc))


def _dedupe_records(records: Sequence[AuditLogRecord]) -> list[AuditLogRecord]:
    seen: set[tuple[str, str, datetime]] = set()
    unique: list[AuditLogRecord] = []
    for record in records:
        key = (record.account_id, record.event_id, record.timestamp)
        if key in seen:
            continue
        seen.add(key)
        unique.append(record)
    return unique


def _group_records_by_hour_and_account(
    records: Sequence[AuditLogRecord],
) -> dict[tuple[datetime, str], list[AuditLogRecord]]:
    grouped: dict[tuple[datetime, str], list[AuditLogRecord]] = {}
    for record in records:
        hour = _normalize_timestamp(record.timestamp).replace(
            minute=0,
            second=0,
            microsecond=0,
        )
        grouped.setdefault((hour, record.account_id), []).append(record)
    return grouped


def _iter_hours(since: datetime, before: datetime) -> list[datetime]:
    start = _normalize_timestamp(since).replace(
        minute=0,
        second=0,
        microsecond=0,
    )
    end = _normalize_timestamp(before)
    if start >= end:
        return []
    hours: list[datetime] = []
    current = start
    while current < end:
        hours.append(current)
        current += timedelta(hours=1)
    return hours


def records_to_parquet_bytes(records: Sequence[AuditLogRecord]) -> bytes:
    if not records:
        raise ObjectStorageError("No records provided for parquet serialization")
    rows = [_record_to_row(record) for record in records]
    return records_to_parquet_bytes_from_rows(rows)


def _build_object_key(
    bucket_time: datetime,
    *,
    prefix: str = "",
    account_id: str,
) -> str:
    path = partition_path(bucket_time, prefix=prefix)
    return f"{path}/account_id={account_id}/part-00000.parquet"


def _record_to_row(record: AuditLogRecord) -> dict[str, Any]:
    return {
        "event_id": record.event_id,
        "timestamp": _normalize_timestamp(record.timestamp),
        "account_id": record.account_id,
        "actor_email": record.actor_email,
        "actor_type": record.actor_type,
        "action_type": record.action_type,
        "action_result": record.action_result,
        "resource_type": record.resource_type,
        "resource_id": record.resource_id,
        "ip_address": record.ip_address,
        "metadata_json": _serialize_json(record.metadata),
        "raw_event_json": _serialize_json(record.raw_event),
        "ingestion_time": _normalize_timestamp(record.ingestion_time),
    }


def _normalize_row(row: Mapping[str, Any]) -> dict[str, Any] | None:
    event_id = row.get("event_id")
    account_id = row.get("account_id")
    timestamp = _normalize_timestamp(row.get("timestamp"))
    if not event_id or not account_id or timestamp is None:
        return None
    ingestion_time = _normalize_timestamp(row.get("ingestion_time")) or timestamp
    return {
        "event_id": str(event_id),
        "timestamp": timestamp,
        "account_id": str(account_id),
        "actor_email": row.get("actor_email"),
        "actor_type": row.get("actor_type"),
        "action_type": row.get("action_type"),
        "action_result": row.get("action_result"),
        "resource_type": row.get("resource_type"),
        "resource_id": row.get("resource_id"),
        "ip_address": row.get("ip_address"),
        "metadata_json": _coerce_json_string(row.get("metadata_json")),
        "raw_event_json": _coerce_json_string(row.get("raw_event_json")),
        "ingestion_time": ingestion_time,
    }


def _row_key(row: Mapping[str, Any]) -> tuple[str, str, datetime]:
    return (
        str(row["account_id"]),
        str(row["event_id"]),
        _normalize_timestamp(row["timestamp"]),
    )


def _dedupe_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str, datetime]] = set()
    unique: list[dict[str, Any]] = []
    for row in rows:
        normalized = _normalize_row(row)
        if normalized is None:
            continue
        key = _row_key(normalized)
        if key in seen:
            continue
        seen.add(key)
        unique.append(normalized)
    unique.sort(key=lambda item: (item["timestamp"], item["event_id"]))
    return unique


def records_to_parquet_bytes_from_rows(rows: Sequence[Mapping[str, Any]]) -> bytes:
    if not rows:
        raise ObjectStorageError("No rows provided for parquet serialization")
    table = pa.Table.from_pylist(list(rows), schema=PARQUET_SCHEMA)
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression=PARQUET_COMPRESSION)
    return sink.getvalue().to_pybytes()


class ObjectStorageClient:
    def __init__(self, *, s3_client, bucket: str, prefix: str = "") -> None:
        if not bucket:
            raise ObjectStorageError("Missing S3 bucket for object storage")
        self._s3_client = s3_client
        self._bucket = bucket
        self._prefix = prefix.strip("/")

    @classmethod
    def from_config(
        cls,
        config: Config,
        *,
        prefix: str | None = None,
    ) -> "ObjectStorageClient":
        if prefix is None:
            prefix = config.s3_prefix
        endpoint = _resolve_s3_endpoint(config.s3_endpoint)
        session = boto3.session.Session(
            aws_access_key_id=config.s3_access_key,
            aws_secret_access_key=config.s3_secret_key,
            region_name=config.s3_region,
        )
        s3_client = session.client(
            "s3",
            endpoint_url=endpoint,
        )
        return cls(s3_client=s3_client, bucket=config.s3_bucket, prefix=prefix)

    def write_records(
        self,
        records: Sequence[AuditLogRecord],
        *,
        run_id: str | None = None,
    ) -> int:
        if not records:
            return 0
        run_id = run_id or uuid4().hex
        deduped = _dedupe_records(records)
        grouped = _group_records_by_hour_and_account(deduped)
        account_count = len({record.account_id for record in deduped})
        logger.info(
            "Writing parquet job_id=%s records=%s account_count=%s hours=%s",
            run_id,
            len(deduped),
            account_count,
            len(grouped),
        )
        written = 0
        for bucket_time, account_id in sorted(grouped.keys()):
            hour_records = sorted(
                grouped[(bucket_time, account_id)],
                key=lambda record: (record.timestamp, record.event_id),
            )
            key = _build_object_key(
                bucket_time,
                prefix=self._prefix,
                account_id=account_id,
            )
            existing_rows = self._read_existing_rows(key)
            combined_rows = _dedupe_rows(
                existing_rows + [_record_to_row(record) for record in hour_records]
            )
            payload = records_to_parquet_bytes_from_rows(combined_rows)
            try:
                self._s3_client.put_object(
                    Bucket=self._bucket,
                    Key=key,
                    Body=payload,
                )
            except Exception as exc:
                raise ObjectStorageError(
                    f"Failed to upload parquet to s3://{self._bucket}/{key}"
                ) from exc
            written += 1
        logger.info(
            "Wrote %s parquet file(s) to object storage job_id=%s",
            written,
            run_id,
        )
        return written

    def _read_existing_rows(self, key: str) -> list[dict[str, Any]]:
        try:
            response = self._s3_client.get_object(
                Bucket=self._bucket,
                Key=key,
            )
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in {"NoSuchKey", "404"}:
                return []
            raise ObjectStorageError(
                f"Failed to read parquet from s3://{self._bucket}/{key}"
            ) from exc
        except Exception as exc:
            raise ObjectStorageError(
                f"Failed to read parquet from s3://{self._bucket}/{key}"
            ) from exc
        body = response["Body"].read()
        table = pq.read_table(pa.BufferReader(body))
        rows = table.to_pylist()
        return [row for row in rows if isinstance(row, dict)]

    def list_keys_for_window(
        self,
        *,
        since: datetime,
        before: datetime,
        account_id: str | None = None,
    ) -> list[str]:
        keys: list[str] = []
        for bucket_time in _iter_hours(since, before):
            prefix = partition_path(bucket_time, prefix=self._prefix)
            hour_keys: list[str] = []
            prefixes = []
            if account_id:
                prefixes.append(f"{prefix}/account_id={account_id}")
            prefixes.append(prefix)
            for candidate in prefixes:
                continuation: str | None = None
                while True:
                    params = {
                        "Bucket": self._bucket,
                        "Prefix": f"{candidate}/",
                    }
                    if continuation:
                        params["ContinuationToken"] = continuation
                    response = self._s3_client.list_objects_v2(**params)
                    for entry in response.get("Contents", []):
                        key = entry.get("Key")
                        if key:
                            hour_keys.append(key)
                    if not response.get("IsTruncated"):
                        break
                    continuation = response.get("NextContinuationToken")
                if hour_keys:
                    break
            keys.extend(hour_keys)
        return sorted(set(keys))

    def read_parquet_records(self, key: str) -> list[dict[str, Any]]:
        try:
            response = self._s3_client.get_object(
                Bucket=self._bucket,
                Key=key,
            )
            body = response["Body"].read()
        except Exception as exc:
            raise ObjectStorageError(
                f"Failed to read parquet from s3://{self._bucket}/{key}"
            ) from exc
        table = pq.read_table(pa.BufferReader(body))
        return table.to_pylist()

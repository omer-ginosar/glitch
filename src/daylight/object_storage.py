from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
from typing import Any, Mapping, Sequence
from uuid import uuid4

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

from daylight.config import Config
from daylight.models import AuditLogRecord


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


@dataclass(frozen=True)
class Partition:
    year: int
    month: int
    day: int
    hour: int


def _normalize_timestamp(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def partition_for_timestamp(value: datetime) -> Partition:
    normalized = _normalize_timestamp(value)
    return Partition(
        year=normalized.year,
        month=normalized.month,
        day=normalized.day,
        hour=normalized.hour,
    )


def partition_path(value: datetime, *, prefix: str = "") -> str:
    partition = partition_for_timestamp(value)
    path = (
        f"year={partition.year:04d}/"
        f"month={partition.month:02d}/"
        f"day={partition.day:02d}/"
        f"hour={partition.hour:02d}"
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


def _dedupe_records(records: Sequence[AuditLogRecord]) -> list[AuditLogRecord]:
    seen: set[tuple[str, datetime]] = set()
    unique: list[AuditLogRecord] = []
    for record in records:
        key = (record.event_id, record.timestamp)
        if key in seen:
            continue
        seen.add(key)
        unique.append(record)
    return unique


def _group_records_by_hour(
    records: Sequence[AuditLogRecord],
) -> dict[datetime, list[AuditLogRecord]]:
    grouped: dict[datetime, list[AuditLogRecord]] = {}
    for record in records:
        hour = _normalize_timestamp(record.timestamp).replace(
            minute=0,
            second=0,
            microsecond=0,
        )
        grouped.setdefault(hour, []).append(record)
    return grouped


def records_to_parquet_bytes(records: Sequence[AuditLogRecord]) -> bytes:
    if not records:
        raise ObjectStorageError("No records provided for parquet serialization")
    rows: list[dict[str, Any]] = []
    for record in records:
        rows.append(
            {
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
        )
    table = pa.Table.from_pylist(rows, schema=PARQUET_SCHEMA)
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression=PARQUET_COMPRESSION)
    return sink.getvalue().to_pybytes()


def _build_object_key(bucket_time: datetime, run_id: str, *, prefix: str = "") -> str:
    path = partition_path(bucket_time, prefix=prefix)
    return f"{path}/part-{run_id}.parquet"


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
        session = boto3.session.Session(
            aws_access_key_id=config.s3_access_key,
            aws_secret_access_key=config.s3_secret_key,
            region_name=config.s3_region,
        )
        s3_client = session.client(
            "s3",
            endpoint_url=config.s3_endpoint,
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
        grouped = _group_records_by_hour(deduped)
        written = 0
        for bucket_time in sorted(grouped.keys()):
            hour_records = sorted(
                grouped[bucket_time],
                key=lambda record: (record.timestamp, record.event_id),
            )
            payload = records_to_parquet_bytes(hour_records)
            key = _build_object_key(bucket_time, run_id, prefix=self._prefix)
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
        logger.info("Wrote %s parquet file(s) to object storage", written)
        return written

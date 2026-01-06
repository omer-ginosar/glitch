from datetime import datetime, timezone
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq

from glitch.models import AuditLogRecord
from glitch.object_storage import (
    ObjectStorageClient,
    partition_path,
    records_to_parquet_bytes,
)


class _StubS3:
    def __init__(self) -> None:
        self.put_calls: list[dict] = []

    def put_object(self, **kwargs):
        self.put_calls.append(kwargs)
        return {}


class _StubBody:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return self._payload


class _StubS3Read:
    def __init__(
        self,
        *,
        keys_by_prefix: Optional[dict[str, list[str]]] = None,
        objects: Optional[dict[str, bytes]] = None,
    ) -> None:
        self._keys_by_prefix = keys_by_prefix or {}
        self._objects = objects or {}
        self.list_calls: list[str] = []
        self.get_calls: list[str] = []

    def list_objects_v2(self, **kwargs):
        prefix = kwargs["Prefix"]
        self.list_calls.append(prefix)
        contents = [
            {"Key": key} for key in self._keys_by_prefix.get(prefix, [])
        ]
        return {"Contents": contents, "IsTruncated": False}

    def get_object(self, **kwargs):
        key = kwargs["Key"]
        self.get_calls.append(key)
        return {"Body": _StubBody(self._objects[key])}


def _make_record(
    event_id: str,
    timestamp: datetime,
    *,
    account_id: str = "acct",
) -> AuditLogRecord:
    return AuditLogRecord(
        event_id=event_id,
        timestamp=timestamp,
        account_id=account_id,
        actor_email="user@example.com",
        actor_type="user",
        action_type="login",
        action_result="success",
        resource_type="zone",
        resource_id="zone-1",
        ip_address="192.0.2.1",
        metadata={"key": "value"},
        ingestion_time=timestamp,
        raw_event={"id": event_id},
    )


def test_partition_path_with_prefix():
    ts = datetime(2025, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    assert (
        partition_path(ts, prefix="audit-logs")
        == "audit-logs/year=2025/month=01/day=02/hour=03"
    )


def test_records_to_parquet_bytes_round_trip():
    ts = datetime(2025, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    record = _make_record("evt-1", ts)
    data = records_to_parquet_bytes([record])
    table = pq.read_table(pa.BufferReader(data))
    assert table.num_rows == 1
    row = table.to_pylist()[0]
    assert row["event_id"] == "evt-1"
    assert row["metadata_json"] == "{\"key\":\"value\"}"
    assert row["raw_event_json"] == "{\"id\":\"evt-1\"}"


def test_write_records_uploads_grouped_by_hour():
    ts1 = datetime(2025, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    ts2 = datetime(2025, 1, 2, 4, 1, 0, tzinfo=timezone.utc)
    records = [_make_record("evt-1", ts1), _make_record("evt-2", ts2)]
    stub = _StubS3()
    client = ObjectStorageClient(s3_client=stub, bucket="audit-logs")
    written = client.write_records(records, run_id="run123")

    assert written == 2
    keys = [call["Key"] for call in stub.put_calls]
    assert (
        "year=2025/month=01/day=02/hour=03/account_id=acct/part-run123.parquet"
        in keys
    )
    assert (
        "year=2025/month=01/day=02/hour=04/account_id=acct/part-run123.parquet"
        in keys
    )


def test_write_records_dedupes_within_poll():
    ts = datetime(2025, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    record = _make_record("evt-1", ts)
    stub = _StubS3()
    client = ObjectStorageClient(s3_client=stub, bucket="audit-logs")
    client.write_records([record, record], run_id="run456")

    assert len(stub.put_calls) == 1
    payload = stub.put_calls[0]["Body"]
    table = pq.read_table(pa.BufferReader(payload))
    assert table.num_rows == 1


def test_list_keys_for_window_by_hour():
    since = datetime(2025, 1, 2, 3, 30, tzinfo=timezone.utc)
    before = datetime(2025, 1, 2, 5, 0, tzinfo=timezone.utc)
    prefix_3 = partition_path(datetime(2025, 1, 2, 3, 0, tzinfo=timezone.utc))
    prefix_4 = partition_path(datetime(2025, 1, 2, 4, 0, tzinfo=timezone.utc))
    keys_by_prefix = {
        f"{prefix_3}/": [f"{prefix_3}/account_id=acct/part-a.parquet"],
        f"{prefix_4}/": [f"{prefix_4}/account_id=acct/part-b.parquet"],
    }
    stub = _StubS3Read(keys_by_prefix=keys_by_prefix)
    client = ObjectStorageClient(s3_client=stub, bucket="audit-logs")

    keys = client.list_keys_for_window(since=since, before=before)

    assert keys == sorted(
        [
            f"{prefix_3}/account_id=acct/part-a.parquet",
            f"{prefix_4}/account_id=acct/part-b.parquet",
        ]
    )
    assert stub.list_calls == [f"{prefix_3}/", f"{prefix_4}/"]


def test_list_keys_for_window_by_hour_and_account():
    since = datetime(2025, 1, 2, 3, 30, tzinfo=timezone.utc)
    before = datetime(2025, 1, 2, 5, 0, tzinfo=timezone.utc)
    prefix_3 = partition_path(datetime(2025, 1, 2, 3, 0, tzinfo=timezone.utc))
    prefix_4 = partition_path(datetime(2025, 1, 2, 4, 0, tzinfo=timezone.utc))
    acct_prefix_3 = f"{prefix_3}/account_id=acct"
    acct_prefix_4 = f"{prefix_4}/account_id=acct"
    keys_by_prefix = {
        f"{acct_prefix_3}/": [f"{acct_prefix_3}/part-a.parquet"],
        f"{acct_prefix_4}/": [f"{acct_prefix_4}/part-b.parquet"],
    }
    stub = _StubS3Read(keys_by_prefix=keys_by_prefix)
    client = ObjectStorageClient(s3_client=stub, bucket="audit-logs")

    keys = client.list_keys_for_window(
        since=since,
        before=before,
        account_id="acct",
    )

    assert keys == sorted(
        [
            f"{acct_prefix_3}/part-a.parquet",
            f"{acct_prefix_4}/part-b.parquet",
        ]
    )
    assert stub.list_calls == [f"{acct_prefix_3}/", f"{acct_prefix_4}/"]


def test_read_parquet_records_from_object_storage():
    ts = datetime(2025, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    record = _make_record("evt-1", ts)
    payload = records_to_parquet_bytes([record])
    key = "year=2025/month=01/day=02/hour=03/account_id=acct/part-test.parquet"
    stub = _StubS3Read(objects={key: payload})
    client = ObjectStorageClient(s3_client=stub, bucket="audit-logs")

    rows = client.read_parquet_records(key)

    assert len(rows) == 1
    assert rows[0]["event_id"] == "evt-1"

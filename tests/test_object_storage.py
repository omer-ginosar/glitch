from datetime import datetime, timezone

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


def _make_record(event_id: str, timestamp: datetime) -> AuditLogRecord:
    return AuditLogRecord(
        event_id=event_id,
        timestamp=timestamp,
        account_id="acct",
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
    assert "year=2025/month=01/day=02/hour=03/part-run123.parquet" in keys
    assert "year=2025/month=01/day=02/hour=04/part-run123.parquet" in keys


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

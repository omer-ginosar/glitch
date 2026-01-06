"""Streaming ingest entry point."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import logging
import os
import time
from uuid import uuid4

import psycopg2

from glitch.cloudflare import CloudflareAPIError, CloudflareClient
from glitch.config import ConfigError, load_config
from glitch.db import (
    connect,
    ensure_audit_logs_table,
    ensure_checkpoint_table,
    insert_audit_logs,
    load_checkpoint,
    save_checkpoint,
)
from glitch.models import AuditLogRecord, SchemaError
from glitch.object_storage import ObjectStorageClient, ObjectStorageError


logger = logging.getLogger(__name__)


def _poll_once(
    client: CloudflareClient,
    *,
    conn,
    account_id: str,
    checkpoint: datetime | None,
    poll_interval_seconds: int,
    safety_lag_seconds: int,
    job_id: str,
    object_storage: ObjectStorageClient | None = None,
) -> tuple[datetime | None, int]:
    now = datetime.now(timezone.utc)
    rewind = timedelta(seconds=safety_lag_seconds)
    since_anchor = checkpoint or (now - timedelta(seconds=poll_interval_seconds))
    since = since_anchor - rewind
    before = now

    latest = checkpoint
    events_seen = 0
    records: list[AuditLogRecord] = []
    for event in client.iter_audit_logs(since=since, before=before):
        try:
            record = AuditLogRecord.from_cloudflare_event(
                event,
                account_id=account_id,
                ingestion_time=now,
            )
        except SchemaError as exc:
            logger.warning("Skipping invalid audit log event: %s", exc)
            continue

        records.append(record)
        events_seen += 1
        if latest is None or record.timestamp > latest:
            latest = record.timestamp

    inserted = 0
    if records:
        with conn:
            inserted = insert_audit_logs(conn, records)
        if object_storage is not None:
            object_storage.write_records(records)
    # Keep the checkpoint trailing so late-arriving events are re-read.
    checkpoint_floor = now - rewind
    if latest is None or latest < checkpoint_floor:
        latest = checkpoint_floor
    if latest is not None:
        with conn:
            save_checkpoint(conn, latest)

    logger.info(
        "Poll complete job_id=%s account_id=%s since=%s before=%s events=%s inserted=%s",
        job_id,
        account_id,
        since,
        before,
        events_seen,
        inserted,
    )
    return latest, events_seen


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    try:
        config = load_config()
    except ConfigError as exc:
        logger.error("Configuration error: %s", exc)
        raise SystemExit(str(exc)) from exc

    client = CloudflareClient(
        account_id=config.cloudflare_account_id,
        api_token=config.cloudflare_api_token,
        per_page=config.cloudflare_per_page,
        direction=config.cloudflare_direction,
        hide_user_logs=config.cloudflare_hide_user_logs,
        timeout_seconds=config.cloudflare_request_timeout_seconds,
        max_retries=config.cloudflare_max_retries,
        backoff_seconds=config.cloudflare_backoff_seconds,
        backoff_max_seconds=config.cloudflare_backoff_max_seconds,
    )
    object_storage = ObjectStorageClient.from_config(config)

    conn = connect(config)
    try:
        with conn:
            ensure_audit_logs_table(conn)
            ensure_checkpoint_table(conn)
        checkpoint = load_checkpoint(conn) or config.initial_checkpoint
    finally:
        conn.close()
    if checkpoint is not None:
        logger.info("Using checkpoint: %s", checkpoint.isoformat())
    while True:
        job_id = uuid4().hex
        logger.info(
            "Starting poll job_id=%s account_id=%s checkpoint=%s",
            job_id,
            config.cloudflare_account_id,
            checkpoint.isoformat() if checkpoint else None,
        )
        conn = None
        try:
            conn = connect(config)
            new_checkpoint, _ = _poll_once(
                client,
                conn=conn,
                account_id=config.cloudflare_account_id,
                checkpoint=checkpoint,
                poll_interval_seconds=config.poll_interval_seconds,
                safety_lag_seconds=config.cloudflare_since_safety_lag_seconds,
                job_id=job_id,
                object_storage=object_storage,
            )
            if new_checkpoint is not None:
                checkpoint = new_checkpoint
        except CloudflareAPIError as exc:
            logger.error("Cloudflare API error job_id=%s: %s", job_id, exc)
        except ObjectStorageError as exc:
            logger.error("Object storage error job_id=%s: %s", job_id, exc)
        except psycopg2.Error as exc:
            logger.error("Database error job_id=%s: %s", job_id, exc)
        except Exception:
            logger.exception("Unexpected ingestion error job_id=%s", job_id)
        finally:
            if conn is not None:
                conn.close()

        time.sleep(config.poll_interval_seconds)


if __name__ == "__main__":
    main()

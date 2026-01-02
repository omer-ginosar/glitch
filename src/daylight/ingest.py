"""Streaming ingest entry point."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import logging
import os
import time

from daylight.cloudflare import CloudflareAPIError, CloudflareClient
from daylight.config import ConfigError, load_config
from daylight.db import (
    connect,
    ensure_audit_logs_table,
    ensure_checkpoint_table,
    insert_audit_logs,
    load_checkpoint,
    save_checkpoint,
)
from daylight.models import AuditLogRecord, SchemaError


logger = logging.getLogger(__name__)


def _poll_once(
    client: CloudflareClient,
    *,
    conn,
    account_id: str,
    checkpoint: datetime | None,
    poll_interval_seconds: int,
    safety_lag_seconds: int,
) -> tuple[datetime | None, int]:
    now = datetime.now(timezone.utc)
    since = checkpoint or (now - timedelta(seconds=poll_interval_seconds))
    since = since - timedelta(seconds=safety_lag_seconds)
    before = now

    latest = checkpoint
    count = 0
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
        count += 1
        if latest is None or record.timestamp > latest:
            latest = record.timestamp

    inserted = 0
    if records:
        inserted = insert_audit_logs(conn, records)
        if latest is not None:
            save_checkpoint(conn, latest)

    logger.info(
        "Fetched %s audit logs between %s and %s (inserted %s)",
        count,
        since,
        before,
        inserted,
    )
    return latest, count


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    try:
        config = load_config()
    except ConfigError as exc:
        raise SystemExit(str(exc)) from exc

    client = CloudflareClient(
        account_id=config.cloudflare_account_id,
        api_token=config.cloudflare_api_token,
        per_page=config.cloudflare_per_page,
        direction=config.cloudflare_direction,
        hide_user_logs=config.cloudflare_hide_user_logs,
        timeout_seconds=config.cloudflare_request_timeout_seconds,
    )

    conn = connect(config)
    with conn:
        ensure_audit_logs_table(conn)
        ensure_checkpoint_table(conn)
    checkpoint = load_checkpoint(conn) or config.initial_checkpoint
    if checkpoint is not None:
        logger.info("Using checkpoint: %s", checkpoint.isoformat())
    while True:
        try:
            with conn:
                new_checkpoint, _ = _poll_once(
                    client,
                    conn=conn,
                    account_id=config.cloudflare_account_id,
                    checkpoint=checkpoint,
                    poll_interval_seconds=config.poll_interval_seconds,
                    safety_lag_seconds=config.cloudflare_since_safety_lag_seconds,
                )
                if new_checkpoint is not None:
                    checkpoint = new_checkpoint
        except CloudflareAPIError as exc:
            logger.error("Cloudflare API error: %s", exc)
        except Exception:
            logger.exception("Unexpected ingestion error")

        time.sleep(config.poll_interval_seconds)


if __name__ == "__main__":
    main()

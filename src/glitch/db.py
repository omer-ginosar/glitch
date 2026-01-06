from __future__ import annotations

import logging
import os
import socket
from typing import Iterable, Sequence

import psycopg2
from psycopg2.extras import Json, execute_values

from glitch.config import Config
from glitch.models import AuditLogRecord


logger = logging.getLogger(__name__)


def _resolve_pg_host(host: str, port: int) -> str:
    if host != "postgres":
        return host
    if os.path.exists("/.dockerenv"):
        return host
    try:
        socket.getaddrinfo(host, port, socket.AF_UNSPEC, socket.SOCK_STREAM)
        return host
    except socket.gaierror:
        return "127.0.0.1"


def connect(config: Config) -> psycopg2.extensions.connection:
    host = _resolve_pg_host(config.pg_host, config.pg_port)
    return psycopg2.connect(
        host=host,
        port=config.pg_port,
        dbname=config.pg_database,
        user=config.pg_user,
        password=config.pg_password,
    )


def ensure_checkpoint_table(conn: psycopg2.extensions.connection) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS ingestion_checkpoint (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                last_timestamp TIMESTAMPTZ NOT NULL
            )
            """
        )


def _ensure_audit_log_table(
    conn: psycopg2.extensions.connection,
    *,
    table_name: str,
) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                event_id TEXT NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL,
                account_id TEXT,
                actor_email TEXT,
                actor_type TEXT,
                action_type TEXT,
                action_result TEXT,
                resource_type TEXT,
                resource_id TEXT,
                ip_address INET,
                metadata JSONB,
                raw_event JSONB,
                ingestion_time TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (timestamp, account_id, event_id)
            )
            """
        )

    with conn.cursor() as cursor:
        cursor.execute("SAVEPOINT create_hypertable")
        try:
            cursor.execute(
                f"SELECT create_hypertable('{table_name}', 'timestamp', if_not_exists => TRUE)"
            )
        except psycopg2.Error as exc:
            cursor.execute("ROLLBACK TO SAVEPOINT create_hypertable")
            logger.warning("Skipping hypertable creation: %s", exc)
        finally:
            cursor.execute("RELEASE SAVEPOINT create_hypertable")


def ensure_audit_logs_table(conn: psycopg2.extensions.connection) -> None:
    _ensure_audit_log_table(conn, table_name="audit_logs")


def ensure_rehydrated_audit_logs_table(
    conn: psycopg2.extensions.connection,
) -> None:
    _ensure_audit_log_table(conn, table_name="audit_logs_rehydrated")


def ensure_audit_logs_view(conn: psycopg2.extensions.connection) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            CREATE OR REPLACE VIEW audit_logs_all AS
            SELECT * FROM audit_logs
            UNION ALL
            SELECT * FROM audit_logs_rehydrated
            """
        )


def load_checkpoint(conn: psycopg2.extensions.connection):
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT last_timestamp FROM ingestion_checkpoint WHERE id = 1"
        )
        row = cursor.fetchone()
    return row[0] if row else None


def save_checkpoint(conn: psycopg2.extensions.connection, timestamp) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO ingestion_checkpoint (id, last_timestamp)
            VALUES (1, %s)
            ON CONFLICT (id) DO UPDATE
            SET last_timestamp = EXCLUDED.last_timestamp
            """,
            (timestamp,),
        )


def _build_audit_log_values(
    records: Sequence[AuditLogRecord],
) -> Iterable[tuple]:
    return [
        (
            record.event_id,
            record.timestamp,
            record.account_id,
            record.actor_email,
            record.actor_type,
            record.action_type,
            record.action_result,
            record.resource_type,
            record.resource_id,
            record.ip_address,
            Json(record.metadata),
            Json(record.raw_event),
            record.ingestion_time,
        )
        for record in records
    ]


def insert_audit_logs(
    conn: psycopg2.extensions.connection,
    records: Sequence[AuditLogRecord],
) -> int:
    if not records:
        return 0

    sql = """
        INSERT INTO audit_logs (
            event_id,
            timestamp,
            account_id,
            actor_email,
            actor_type,
            action_type,
            action_result,
            resource_type,
            resource_id,
            ip_address,
            metadata,
            raw_event,
            ingestion_time
        )
        VALUES %s
        ON CONFLICT DO NOTHING
    """

    with conn.cursor() as cursor:
        execute_values(cursor, sql, _build_audit_log_values(records), page_size=500)
        return cursor.rowcount


def insert_rehydrated_audit_logs(
    conn: psycopg2.extensions.connection,
    records: Sequence[AuditLogRecord],
) -> int:
    if not records:
        return 0

    sql = """
        INSERT INTO audit_logs_rehydrated (
            event_id,
            timestamp,
            account_id,
            actor_email,
            actor_type,
            action_type,
            action_result,
            resource_type,
            resource_id,
            ip_address,
            metadata,
            raw_event,
            ingestion_time
        )
        SELECT
            event_id,
            timestamp,
            account_id,
            actor_email,
            actor_type,
            action_type,
            action_result,
            resource_type,
            resource_id,
            NULLIF(ip_address, '')::inet,
            metadata::jsonb,
            raw_event::jsonb,
            ingestion_time
        FROM (VALUES %s) AS v(
            event_id,
            timestamp,
            account_id,
            actor_email,
            actor_type,
            action_type,
            action_result,
            resource_type,
            resource_id,
            ip_address,
            metadata,
            raw_event,
            ingestion_time
        )
        WHERE NOT EXISTS (
            SELECT 1
            FROM audit_logs
            WHERE audit_logs.timestamp = v.timestamp
            AND audit_logs.event_id = v.event_id
            AND audit_logs.account_id = v.account_id
        )
        ON CONFLICT DO NOTHING
    """

    with conn.cursor() as cursor:
        execute_values(cursor, sql, _build_audit_log_values(records), page_size=500)
        return cursor.rowcount

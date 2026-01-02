from __future__ import annotations

import logging
from typing import Iterable, Sequence

import psycopg2
from psycopg2.extras import Json, execute_values

from daylight.config import Config
from daylight.models import AuditLogRecord


logger = logging.getLogger(__name__)


def connect(config: Config) -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=config.pg_host,
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


def ensure_audit_logs_table(conn: psycopg2.extensions.connection) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS audit_logs (
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
                PRIMARY KEY (timestamp, event_id)
            )
            """
        )

    with conn.cursor() as cursor:
        cursor.execute("SAVEPOINT create_hypertable")
        try:
            cursor.execute(
                "SELECT create_hypertable('audit_logs', 'timestamp', if_not_exists => TRUE)"
            )
        except psycopg2.Error as exc:
            cursor.execute("ROLLBACK TO SAVEPOINT create_hypertable")
            logger.warning("Skipping hypertable creation: %s", exc)
        finally:
            cursor.execute("RELEASE SAVEPOINT create_hypertable")


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


def insert_audit_logs(
    conn: psycopg2.extensions.connection,
    records: Sequence[AuditLogRecord],
) -> int:
    if not records:
        return 0

    values: Iterable[tuple] = [
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
        execute_values(cursor, sql, values, page_size=500)
        return cursor.rowcount

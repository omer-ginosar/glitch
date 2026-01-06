# Architecture

This repository implements a production-ready Cloudflare Audit Logs pipeline. The goal is a simple,
recoverable system that is easy to operate.

## Overview
```text
┌─────────────────┐
│ Cloudflare API  │
└────────┬────────┘
         │ (poll every 60s)
         ▼
┌──────────────────────────┐
│ Python ingestor           │
│ (with checkpointing)     │
└──────┬──────────┬────────┘
       │          │ (sync writes)
       ▼          ▼
┌───────────┐  ┌─────────────────┐
│ PostgreSQL│  │ MinIO/Parquet   │
│ +Timescale│  │ (time+account)  │
└───────────┘  └─────────────────┘
       │
       │ (queries)
       ▼
┌──────────────┐
│ SQL/BI Tools │
└──────────────┘
```

## Design principles
- Simple but not naive: production-ready without over-engineering.
- At-least-once semantics with dedupe: no duplicates, minimize data loss.
- Observable: easy to monitor and debug.
- Recoverable: handles failures gracefully.
- Maintainable: team can support it at 3am.

## Technology choices
### Ingestion: Python polling
- **Choice:** Python 3.11+ with a simple polling loop.
- **Why it works:**
  - Rich ecosystem (urllib, psycopg2, boto3, pyarrow).
  - Easy to debug and modify.
  - Low resource footprint.
- **State management:**
  - Store the last processed timestamp in PostgreSQL.
  - Enables at-least-once processing with dedupe.
  - Survives restarts.
- **Alternatives considered:**
  - Apache Kafka: overkill for a single API source and adds operational complexity.
  - Airflow: batch-oriented, does not give near real-time.
  - AWS Lambda: vendor lock-in, cold starts, and state management complexity.

### Analytics database: PostgreSQL + TimescaleDB
- **Choice:** TimescaleDB (PostgreSQL extension).
- **Why it works:**
  - Time-series optimized for timestamped data.
  - Automatic partitioning for large tables.
  - Standard SQL.
  - Continuous aggregates for dashboards.
  - Compression and retention policies.
  - Single binary, no cluster complexity.
- **Performance characteristics:**
  - Handles 50k+ inserts/sec on modest hardware.
  - Sub-100ms query latency for typical audit queries.
  - Automatic chunk management (1 day chunks).
- **Alternatives considered:**
  - ClickHouse: steeper learning curve.
  - Elasticsearch: resource-hungry, complex cluster management.
  - MongoDB: not optimized for time-series analytics.

### Object storage: MinIO + Parquet
- **Choice:** MinIO with Parquet files.
- **Why Parquet:**
  - Columnar format for compression and fast analytics.
  - Widely supported by pandas, Spark, Athena, DuckDB.
  - Schema evolution with additive columns.
  - Predicate pushdown for selective reads.
  - Simple file-based storage without a catalog.
- **Why MinIO:**
  - S3-compatible with the broader ecosystem.
  - Single binary, easy to deploy.
  - High throughput.
  - No vendor lock-in; can move to AWS S3.
- **Partitioning strategy:**
  ```text
  s3://audit-logs/
    year=2025/
      month=01/
        day=01/
          hour=00/
            account_id=acct/
              part-00000.parquet (1 hour of data, merged/overwritten)
  ```
- **Alternatives considered:**
  - Apache Iceberg: adds catalog complexity, overkill for this use case.
  - Delta Lake: Databricks-centric, Java dependency.
  - ORC: similar to Parquet but less Python-friendly.

### Deployment: Docker Compose + systemd
- **Choice:** Docker Compose for local/dev, systemd for production.
- **Why it works:**
  - Portable, version-controlled infrastructure.
  - Isolated containers prevent dependency conflicts.
  - Easy rollbacks by changing image tags.
- **Alternatives considered:**
  - Kubernetes: massive overhead for 3-4 services.
  - Bare metal: dependency conflicts, hard to reproduce.

## Data model
### PostgreSQL schema
```sql
-- Main table with TimescaleDB hypertable
CREATE TABLE audit_logs (
  -- Primary key
  event_id TEXT NOT NULL,
  timestamp TIMESTAMPTZ NOT NULL,

  -- Structured fields (for fast queries)
  account_id TEXT,
  actor_email TEXT,
  actor_type TEXT,
  action_type TEXT,
  action_result TEXT,
  resource_type TEXT,
  resource_id TEXT,
  ip_address INET,

  -- Semi-structured fields (often queried, but not fixed schema)
  metadata JSONB,

  -- Flexible storage for evolving schema
  raw_event JSONB,

  -- Pipeline metadata
  ingestion_time TIMESTAMPTZ DEFAULT NOW(),

  PRIMARY KEY (timestamp, account_id, event_id)
);

-- Convert to hypertable (automatic time-based partitioning)
SELECT create_hypertable('audit_logs', 'timestamp');
```

**Key decisions**
- Hybrid schema: structured fields for common queries plus JSONB for flexibility.
- Composite primary key (timestamp, account_id, event_id) enables time partitioning.
- INET type for native IP address queries.
- JSONB keeps full API responses for future analysis.
- metadata JSONB is extracted for quick filters; raw_event preserves the full event.

### Field mapping (Cloudflare -> internal)
| Internal field | Cloudflare field | Notes |
| --- | --- | --- |
| event_id | id | Primary identifier. |
| timestamp | when | Event time. |
| account_id | account_id | From request path. |
| actor_email | actor.email | Actor email address. |
| actor_type | actor.type | Actor type. |
| action_type | action.type | Action type. |
| action_result | action.result | Success or failure. |
| resource_type | resource.type | Resource type. |
| resource_id | resource.id | Resource identifier. |
| ip_address | actor.ip | Stored as INET in Postgres. |
| metadata | metadata | JSON object. |

### Parquet schema (MinIO)
| Column | Type | Notes |
| --- | --- | --- |
| event_id | string | Primary identifier. |
| timestamp | timestamp[ns] | Event time. |
| account_id | string | Cloudflare account. |
| actor_email | string | Actor email address. |
| actor_type | string | Actor type. |
| action_type | string | Action type. |
| action_result | string | Success or failure. |
| resource_type | string | Resource type. |
| resource_id | string | Resource identifier. |
| ip_address | string | Stored as text for portability. |
| metadata_json | string | JSON string. |
| raw_event_json | string | JSON string. |
| ingestion_time | timestamp[ns] | Ingestion time. |

**Partitioning**
- year/month/day/hour/account_id for optimal query performance.

## Why polling (not webhooks)
- Cloudflare Audit Logs API is designed for incremental polling by time.
- Polling keeps the ingestion loop simple and predictable.
- Avoids webhook retries, auth rotation, and public endpoint hosting.

## Ingestion loop contract
| Aspect | Details |
| --- | --- |
| Inputs (env) | CLOUDFLARE_API_TOKEN, CLOUDFLARE_ACCOUNT_ID, POLL_INTERVAL_SECONDS |
| Optional inputs | CLOUDFLARE_PER_PAGE, CLOUDFLARE_DIRECTION, CLOUDFLARE_SINCE_SAFETY_LAG_SECONDS, CLOUDFLARE_HIDE_USER_LOGS, CLOUDFLARE_REQUEST_TIMEOUT_SECONDS, CLOUDFLARE_MAX_RETRIES, CLOUDFLARE_BACKOFF_SECONDS, CLOUDFLARE_BACKOFF_MAX_SECONDS, INITIAL_CHECKPOINT |
| Checkpoint | Stored in PostgreSQL as the latest processed `when` timestamp |
| Time window | since = checkpoint - safety lag, before = now, direction = asc |
| Pagination | Request page=1..N; stop when result count < per_page |
| Ingest | Map fields, store raw_event JSONB, insert with ON CONFLICT DO NOTHING |
| Commit | Advance checkpoint to max(event timestamp, now - safety lag) after successful writes |
| Failure | Do not advance checkpoint on API or sink errors; retry on next poll |

## State and checkpointing
- The ingestor stores the last processed timestamp in PostgreSQL.
- On each run, it queries the API for events newer than the checkpoint (with a safety lag).
- If no checkpoint exists, start at now minus the poll interval.
- The checkpoint is advanced only after successful writes to both sinks, and never beyond (now - safety lag)
  to preserve a late-arrival window.

## Duplicate handling
- PostgreSQL primary key on (timestamp, account_id, event_id) prevents duplicates.
- If a poll window overlaps, duplicate inserts are ignored.
- Parquet writes are partitioned by time plus account_id and merged into a deterministic key to keep idempotency.

## Retention and compression (optional)
- TimescaleDB retention: keep 90 days of data (drop older chunks).
- TimescaleDB compression: compress chunks older than 30 days.

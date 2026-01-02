# Architecture

This repository follows the provided architecture exactly:

Production-Ready Cloudflare Audit Logs Pipeline - Complete Plan
I'll design a pragmatic, production-grade solution that balances simplicity with real-world requirements.
Architecture Overview


┌─────────────────┐
│ Cloudflare API │
└────────┬────────┘
│ (Poll every 60s)
▼
┌─────────────────────────┐
│ Python Ingestor │
│ (with checkpointing) │
└──────┬──────────┬───────┘
│ │
│ │ (async writes)
▼ ▼
┌──────────┐ ┌────────────────┐
│PostgreSQL│ │ MinIO/Parquet │
│+ Timescale│ │ (partitioned) │
└──────────┘ └────────────────┘
│
│ (queries)
▼
┌──────────┐
│ Grafana │
└──────────┘
Key Design Principles
Simple but not naive - Production-ready without over-engineering
Exactly-once semantics - No duplicates, no data loss
Observable - Easy to monitor and debug
Recoverable - Handles failures gracefully
Maintainable - Team can support it at 3am

Technology Stack & Justification
1. Ingestion: Python with State Management
Choice: Python 3.11+ with asyncio
Why:
✅ Native async/await for concurrent API calls and DB writes
✅ Rich ecosystem (requests, pandas, psycopg2, s3fs)
✅ Easy to debug and modify
✅ Low resource footprint
State Management:
Store last processed timestamp in PostgreSQL
Enables exactly-once processing
Survives restarts
Alternatives Rejected:
❌ Apache Kafka - Overkill for single API source, adds operational complexity
❌ Airflow - Batch-oriented, doesn't give us "near real-time"
❌ AWS Lambda - Vendor lock-in, cold starts, state management complexity
2. Analytical Database: PostgreSQL + TimescaleDB
Choice: TimescaleDB (PostgreSQL extension)
Why:
✅ Time-series optimized - Built for timestamped data
✅ Automatic partitioning - Handles billions of rows efficiently
✅ Standard SQL - Everyone knows it
✅ Continuous aggregates - Pre-computed metrics for dashboards
✅ Compression - 90%+ storage reduction on old data
✅ Retention policies - Auto-delete old data
✅ Single binary - No cluster complexity
Performance Characteristics:
Handles 50K+ inserts/sec on modest hardware
Sub-100ms query latency for typical audit queries
Automatic chunk management (1 day chunks)
Alternatives Rejected:
❌ ClickHouse - Steeper learning curve, less familiar to teams
❌ Elasticsearch - Resource-hungry, complex cluster management
❌ MongoDB - Not optimized for time-series analytics
3. Object Storage: MinIO + Parquet
Choice: MinIO with Parquet files
Why Parquet:
✅ Columnar format - 10x compression, fast analytics
✅ Widely supported - Works with pandas, Spark, Athena, DuckDB
✅ Schema evolution - Can add columns without rewriting
✅ Predicate pushdown - Read only needed data
✅ Simple - Just files, no catalog overhead
Why MinIO:
✅ S3-compatible - Works with entire S3 ecosystem
✅ Single binary - Easy to deploy
✅ High performance - Multi-GB/s throughput
✅ No vendor lock-in - Can move to AWS S3 anytime
Partitioning Strategy:


s3://audit-logs/
year=2025/
month=01/
day=01/
hour=00/
part-0000.parquet (1 hour of data)
part-0001.parquet
Alternatives Rejected:
❌ Apache Iceberg - Adds catalog complexity, overkill for our use case
❌ Delta Lake - Databricks-centric, Java dependency
❌ ORC - Similar to Parquet but less Python-friendly
4. Deployment: Docker Compose + Systemd
Choice: Docker Compose for local/dev, systemd for production
Why:
✅ Portable - Same setup everywhere
✅ Version controlled - Infrastructure as code
✅ Isolated - Containers prevent dependency conflicts
✅ Easy rollback - Just change image tag
Alternatives Rejected:
❌ Kubernetes - Massive overhead for 3-4 services
❌ Bare metal - Dependency hell, hard to reproduce

Data Modeling Strategy
Schema Design (PostgreSQL)


sql
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

PRIMARY KEY (timestamp, event_id)
);

-- Convert to hypertable (automatic time-based partitioning)
SELECT create_hypertable('audit_logs', 'timestamp');
Key Decisions:
Hybrid schema: Structured fields for common queries + JSONB for flexibility
Composite PK: (timestamp, event_id) enables time-based partitioning
INET type: Native IP address type for network queries
JSONB: Full API response preserved for future analysis
Metadata JSONB: Extracted from the event for quick filters; full event still stored

Field Mapping (Cloudflare -> Internal)
event_id: id
timestamp: when
account_id: account_id from request path
actor_email: actor.email
actor_type: actor.type
action_type: action.type
action_result: action.result
resource_type: resource.type
resource_id: resource.id
ip_address: actor.ip
metadata: metadata (JSON object)
Parquet Schema (MinIO)


python
# Flatten structure for analytics
{
"event_id": "string",
"timestamp": "timestamp[ns]",
"account_id": "string",
"actor_email": "string",
"actor_type": "string",
"action_type": "string",
"action_result": "string",
"resource_type": "string",
"resource_id": "string",
"ip_address": "string",
"metadata": "string", # JSON string
"ingestion_time": "timestamp[ns]"
}
Partitioning: year/month/day/hour for optimal query performance

## Why polling (not webhooks)
- Cloudflare Audit Logs API is designed for incremental polling by time.
- Polling keeps the ingestion loop simple and predictable for a home assignment.
- It avoids webhook delivery retries, auth rotation, and public endpoint hosting.

## Ingestion loop contract
- Inputs (env): CLOUDFLARE_API_TOKEN, CLOUDFLARE_ACCOUNT_ID, POLL_INTERVAL_SECONDS
- Optional inputs: CLOUDFLARE_PER_PAGE, CLOUDFLARE_DIRECTION, CLOUDFLARE_SINCE_SAFETY_LAG_SECONDS, CLOUDFLARE_HIDE_USER_LOGS, CLOUDFLARE_REQUEST_TIMEOUT_SECONDS, INITIAL_CHECKPOINT
- Checkpoint: stored in PostgreSQL as the latest processed `when` timestamp
- Time window: since = checkpoint - safety lag, before = now, direction = asc
- Pagination: request page=1..N; stop when result count < per_page (no result_info returned)
- Ingest: map fields, store raw_event JSONB, insert with ON CONFLICT DO NOTHING
- Commit: advance checkpoint to max event timestamp after processing events (even if inserts are deduped)
- Failure: do not advance checkpoint on API or sink errors; retry on next poll

## State and checkpointing
- The ingestor stores the last processed timestamp in PostgreSQL.
- On each run, it queries the API for events newer than the checkpoint (with a safety lag).
- If no checkpoint exists, start at now minus the poll interval.
- The checkpoint is advanced only after successful writes to both sinks.

## Duplicate handling
- PostgreSQL primary key on (timestamp, event_id) prevents duplicates.
- If a poll window overlaps, duplicate inserts are ignored or upserted.
- Parquet writes are partitioned by time; files are written once per window.

## Retention and compression (optional)
- TimescaleDB retention: keep 90 days of data (drop older chunks).
- TimescaleDB compression: compress chunks older than 30 days.

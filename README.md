# Cloudflare Audit Logs Pipeline (Home Assignment)

- Polls Cloudflare Audit Logs API continuously (every N seconds).
- Ingests events with checkpointing and a safety lag window to reduce gaps across restarts.
- Writes near-real-time events to an analytics DB (PostgreSQL + TimescaleDB).
- Writes Parquet files to S3-compatible storage (e.g., MinIO), merged/overwritten per hour+account.
- At-least-once delivery with dedupe in Postgres and idempotent object storage writes.
- Keeps a minimal, debuggable Python ingestion loop.
- Preserves raw event payloads for flexible querying.

## Data flow (high level)
1) Poll Cloudflare Audit Logs API for new events since last checkpoint.
2) Persist events into PostgreSQL/TimescaleDB for fast queries.
3) Persist the same events to S3-compatible storage as Parquet partitions (time+account).

## Configuration
- For Docker Compose, edit `.env` (or copy `.env.example` as a template).
- For non-Docker local runs, copy `config.example.env` to `.env` and adjust host/port values.
- Required: `CLOUDFLARE_API_TOKEN`, `CLOUDFLARE_ACCOUNT_ID`, `POLL_INTERVAL_SECONDS`, `PG_*`, `S3_*`.
- Optional: `CLOUDFLARE_PER_PAGE`, `CLOUDFLARE_DIRECTION`, `CLOUDFLARE_HIDE_USER_LOGS`, `CLOUDFLARE_SINCE_SAFETY_LAG_SECONDS`, `CLOUDFLARE_REQUEST_TIMEOUT_SECONDS`, `CLOUDFLARE_MAX_RETRIES`, `CLOUDFLARE_BACKOFF_SECONDS`, `CLOUDFLARE_BACKOFF_MAX_SECONDS`, `INITIAL_CHECKPOINT`, `S3_PREFIX`.
- Secrets: keep tokens in `.env` (gitignored) for dev; use a secrets manager, Docker secrets, or systemd credentials in production.

## Setup on a new machine
1) Install dependencies:
   - Python 3.11+
   - Docker + Docker Compose (for local DB + MinIO)
2) Clone the repo and copy config:
   - `cp config.example.env .env`
3) Fill in `.env` values:
   - `CLOUDFLARE_API_TOKEN`, `CLOUDFLARE_ACCOUNT_ID`
   - `PG_*`, `S3_*`, `POLL_INTERVAL_SECONDS`
4) Start services (TimescaleDB + MinIO) with Docker:
   - `docker compose up --build`
5) Run the ingestor:
   - `PYTHONPATH=src python3 -m glitch.ingest`

Notes:
- Compose uses TimescaleDB and creates the MinIO bucket via a `minio-init` job.
- `S3_PREFIX` scopes object storage paths (e.g., `dev/audit-logs`).
- For host-run tests, `.env` should point to `localhost` for Postgres/MinIO; the ingestor container overrides these to reach `postgres`/`minio` inside Compose.

## Postgres/Timescale setup (optional)
TimescaleDB is optional. If installed, enable it and convert the table:

```sql
CREATE EXTENSION IF NOT EXISTS timescaledb;
SELECT create_hypertable('audit_logs', 'timestamp', if_not_exists => TRUE);
```

Recommended policies (optional):

```sql
SELECT add_retention_policy('audit_logs', INTERVAL '90 days');
ALTER TABLE audit_logs SET (timescaledb.compress);
SELECT add_compression_policy('audit_logs', INTERVAL '30 days');
```

## Manual rehydration (CLI)
Rehydrate audit logs from object storage into the DB for a specific time window:

```bash
PYTHONPATH=src python3 -m glitch.rehydrate \
  --since 2025-01-01T00:00:00Z \
  --before 2025-01-02T00:00:00Z
```

This writes to `audit_logs_rehydrated` and exposes a unified view `audit_logs_all`.

## How to test
Run the test suite:

```bash
python3 -m pytest
```

Integration test (runs inside the compose network):

```bash
docker compose run --rm --no-deps -v "$PWD":/app -w /app \
  --entrypoint python3 ingestor -m pytest -m integration \
  tests/test_rehydrate_integration.py
```

# Cloudflare Audit Logs Pipeline (Home Assignment)

- Polls Cloudflare Audit Logs API continuously (every N seconds).
- Ingests events with checkpointing to avoid gaps across restarts.
- Writes near-real-time events to an analytics DB (PostgreSQL + TimescaleDB).
- Writes batch Parquet files to S3-compatible storage (e.g., MinIO).
- Keeps a minimal, debuggable Python ingestion loop.
- Preserves raw event payloads for flexible querying.

## Data flow (high level)
1) Poll Cloudflare Audit Logs API for new events since last checkpoint.
2) Persist events into PostgreSQL/TimescaleDB for fast queries.
3) Persist the same events to S3-compatible storage as Parquet partitions.

## Configuration
- For Docker Compose, edit `.env` (or copy `.env.example` as a template).
- For non-Docker local runs, copy `config.example.env` to `.env` and adjust host/port values.
- Required: `CLOUDFLARE_API_TOKEN`, `CLOUDFLARE_ACCOUNT_ID`, `POLL_INTERVAL_SECONDS`, `PG_*`, `S3_*`.
- Optional: `CLOUDFLARE_PER_PAGE`, `CLOUDFLARE_DIRECTION`, `CLOUDFLARE_HIDE_USER_LOGS`, `CLOUDFLARE_SINCE_SAFETY_LAG_SECONDS`, `CLOUDFLARE_REQUEST_TIMEOUT_SECONDS`, `INITIAL_CHECKPOINT`, `S3_PREFIX`.
- Secrets: keep tokens in `.env` (gitignored) for dev; use a secrets manager, Docker secrets, or systemd credentials in production.

## Docker compose quickstart
1) Fill in `.env` (Cloudflare token/account ID).
2) Run `docker compose up --build`.
3) Stop with `docker compose down`.
4) If you need a clean DB, run `docker compose down -v` to recreate volumes.

Notes:
- Compose uses TimescaleDB and creates the MinIO bucket via a `minio-init` job.
- `S3_PREFIX` scopes object storage paths (e.g., `dev/audit-logs`).

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

## How to run locally
1) Copy `config.example.env` to `.env` and fill in credentials.
2) Start local services (PostgreSQL/TimescaleDB + MinIO) via your preferred setup.
3) Run the Python ingestor: `PYTHONPATH=src python3 -m glitch.ingest`.

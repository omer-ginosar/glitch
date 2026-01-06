# ☁️ Cloudflare Audit Logs Pipeline

A small, production-minded pipeline that continuously ingests Cloudflare audit logs and makes them queryable in Postgres while also storing durable Parquet files in S3-compatible storage.

**What this delivers**
- Near-real-time analytics in PostgreSQL/TimescaleDB.
- Long-term storage in Parquet (time + account partitioning).
- At-least-once ingestion with DB dedupe and idempotent S3 merge/overwrite.
- Clear operational story: checkpoints, safety lag, retries, and rehydration.

## 📎 Key docs
- `architecture.md` covers system design and tradeoffs.
- `assumptions.md` documents scope and future upgrades.

## 🧭 How it works
1) Poll Cloudflare Audit Logs API for new events since the last checkpoint.
2) Insert into Postgres for fast querying (dedupe on primary key).
3) Merge and overwrite hourly Parquet files in S3/MinIO for long-term storage.

## 🗂 Repo structure
- `src/glitch/ingest.py`: polling loop + checkpointing.
- `src/glitch/cloudflare.py`: API client (retries/backoff).
- `src/glitch/db.py`: Postgres schema and inserts.
- `src/glitch/object_storage.py`: Parquet + S3 merge/overwrite.
- `src/glitch/rehydrate.py`: replay from object storage into DB.
- `tests/`: unit tests + integration rehydrate test.
- `architecture.md`: design decisions and data model.
- `assumptions.md`: scope assumptions and future upgrades.
- `.env.example`: Docker Compose configuration template.
- `config.example.env`: local (host-run) configuration template.

## 🔧 Configuration
- For Docker Compose, copy `.env.example` to `.env` and edit values.
- For non-Docker local runs, copy `config.example.env` to `.env` and adjust host/port values.
- Required: `CLOUDFLARE_API_TOKEN`, `CLOUDFLARE_ACCOUNT_ID`, `POLL_INTERVAL_SECONDS`, `PG_*`, `S3_*`.
- Optional: `CLOUDFLARE_PER_PAGE`, `CLOUDFLARE_DIRECTION`, `CLOUDFLARE_HIDE_USER_LOGS`,
  `CLOUDFLARE_SINCE_SAFETY_LAG_SECONDS`, `CLOUDFLARE_REQUEST_TIMEOUT_SECONDS`,
  `CLOUDFLARE_MAX_RETRIES`, `CLOUDFLARE_BACKOFF_SECONDS`, `CLOUDFLARE_BACKOFF_MAX_SECONDS`,
  `INITIAL_CHECKPOINT`, `S3_PREFIX`.

Note: For host-run tests, `.env` should point to `localhost`. The ingestor container
overrides `PG_HOST`/`S3_ENDPOINT` to reach `postgres`/`minio` inside Compose.

## 🚀 Run end-to-end
1) Install prerequisites:
   - Python 3.11+
   - Docker + Docker Compose
2) Create config:
   - `cp .env.example .env`
3) Fill `.env` with:
   - `CLOUDFLARE_API_TOKEN`, `CLOUDFLARE_ACCOUNT_ID`
   - `PG_*`, `S3_*`, `POLL_INTERVAL_SECONDS`
4) Start services:
   - `docker compose up --build`
5) Run the ingestor:
   - `PYTHONPATH=src python3 -m glitch.ingest`

## ♻️ Rehydrate (CLI)
Rehydrate a time window from object storage back into Postgres:

```bash
PYTHONPATH=src python3 -m glitch.rehydrate \
  --since 2025-01-01T00:00:00Z \
  --before 2025-01-02T00:00:00Z
```

This writes to `audit_logs_rehydrated` and exposes a unified view `audit_logs_all`.

## 🧪 Tests
Run unit tests:

```bash
python3 -m pytest
```

Integration test (inside Compose network):

```bash
docker compose run --rm --no-deps -v "$PWD":/app -w /app \
  --entrypoint python3 ingestor -m pytest -m integration \
  tests/test_rehydrate_integration.py
```

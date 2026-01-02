# Assumptions

- Single Cloudflare account and one API token with audit log access.
- Polling interval is fixed and configured via environment variable.
- Volume is moderate (fits in a single Postgres/Timescale instance).
- Exactly-once is achieved by a timestamp checkpoint + primary key constraint.
- No backfill tooling beyond restarting with an earlier checkpoint.
- No schema evolution handling beyond storing raw_event JSONB.
- Minimal security: credentials are passed via environment variables.
- Error handling is basic (log and retry on next poll).
- Parquet files are written per-hour; no compaction or catalog.
- TimescaleDB is optional; retention/compression policies may be enabled in DB.

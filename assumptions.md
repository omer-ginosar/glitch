# Assumptions

- Single Cloudflare account and one API token with audit log access.
- Polling interval is fixed and configured via environment variable.
- Volume is moderate (fits in a single Postgres/Timescale instance).
- Exactly-once is achieved by a timestamp checkpoint + primary key constraint.
- No backfill tooling beyond restarting with an earlier checkpoint.
- No schema evolution handling beyond storing raw_event JSONB.
- Minimal security: credentials are passed via environment variables.
- Error handling is basic (log and retry on next poll).
- INFO+ logs from the ingestor/rehydrator are captured from stdout/stderr by the monitoring stack.
- Parquet files are written per-hour; no compaction or catalog.
- TimescaleDB is optional; retention/compression policies may be enabled in DB.

## Future upgrades if assumptions change

- If multiple accounts are onboarded, deploy multiple workers (e.g., AWS Fargate or equivalent managed container service) and shard ingestion by account_id with per-account checkpoints and token-level rate limiting/backoff.
- If per-account throughput grows, use account_id + time partitioning
  (multidimensional hypertables) to reduce hot chunks and improve query pruning.
- If a single Postgres instance saturates even with parallel workers, reduce retention and compression thesholds to move more hot data to the cold tier.
- If schema evolution becomes frequent, use Alembic for versioned, additive migrations with backfills and expand JSONB indexing.
- If low-latency analytics become critical, introduce rollups/continuous aggregates or a serving layer.
- If query composition grows complex, consider SQLAlchemy Core or an ORM for read paths.

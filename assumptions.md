# Assumptions

- Single Cloudflare account and one API token with audit log access.
- Polling interval is fixed and configured via environment variable.
- Volume is moderate (fits in a single Postgres/Timescale instance).
- At-least-once ingestion with DB dedupe and deterministic per-hour+account S3 merge/overwrite.
- Safety lag window is sized to expected event delays; late arrivals can be recovered by replaying with an earlier checkpoint.
- No automated backfill tooling; manual replay is done by adjusting the checkpoint.
- No schema evolution handling beyond storing raw_event JSONB.
- Minimal security: credentials are passed via environment variables.
- Error handling is basic (log, retry/backoff for API, reconnect DB per poll).
- INFO+ logs from the ingestor/rehydrator are captured from stdout/stderr by the monitoring stack.
- Parquet files are written per-hour+account and overwritten with merged data (idempotent); no compaction or catalog.
- TimescaleDB is optional; retention/compression policies may be enabled in DB.

## Future upgrades if assumptions change

- If multiple accounts are onboarded, deploy multiple workers (e.g., AWS Fargate or equivalent managed container service) and shard ingestion by account_id with per-account checkpoints and token-level rate limiting/backoff.
- If per-account throughput grows, use account_id + time partitioning
  (multidimensional hypertables) to reduce hot chunks and improve query pruning.
- If a single Postgres instance saturates even with parallel workers, reduce retention and compression thesholds to move more hot data to the cold tier. its are consistently hit, implement adaptive backoff with jitter and add per-account throttling.
- If scale/limits change, add adaptive backoff with jitter, per-account throttling/parallelism, and basic metrics/health checks to keep up with API volume and rate limits.
- If storage/rehydration grows, add compaction to avoid small-file explosion, stream row groups or add pushdown filters to reduce memory, and consider Parquet struct/map types.
- If schema evolution becomes frequent, use Alembic for versioned, additive migrations with backfills and expand JSONB indexing.
- If low-latency analytics become critical, introduce rollups/continuous aggregates or a serving layer.
- If query composition grows complex, consider SQLAlchemy Core or an ORM for read paths.
- Add ingestion loop tests and a replay/backfill CLI; add DB indexes/partial indexes and widen safety lag or replay windows for late-arriving events.

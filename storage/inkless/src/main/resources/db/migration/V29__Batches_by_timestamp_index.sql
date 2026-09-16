-- Copyright (c) 2026 Aiven, Helsinki, Finland. https://aiven.io/

-- Index the effective batch timestamp so list_offsets_v1's timestamp branches stop scanning the partition.
--
-- MAX_TIMESTAMP and the concrete-timestamp lookup (offsetsForTimes) filter and aggregate on
-- batch_timestamp(timestamp_type, batch_max_timestamp, log_append_timestamp). No index covers that
-- expression, so both walk every retained batch of the partition (or, for the ORDER BY batch_id form,
-- every batch in the table) and evaluate the function per row. Cost is proportional to retained batches,
-- and a partition with a large retention or prune backlog pushes a single ListOffsets past
-- statement_timeout. With this index, MAX is a backward range scan and the concrete-timestamp lookup
-- (rewritten in V30 as MIN(base_offset) over the predicate) is a range scan over the batches at or after
-- the target. INCLUDE (base_offset) is what lets that MIN read the index alone; without it every matched
-- entry costs a heap fetch and the planner may fall back to a full partition scan through
-- batches_by_last_offset_covering_idx under a generic plan.
--
-- The expression must stay byte-identical in the queries for the planner to match it, and
-- batch_timestamp must stay IMMUTABLE and NON-INLINABLE (LANGUAGE plpgsql). The index stores the
-- expression as a function call; if the function is later made LANGUAGE sql, every query inlines it to a
-- bare CASE that no longer matches the stored expression, and the index is silently unused. Rewriting
-- the function requires rebuilding this index. The index also depends on the function, so the function
-- cannot be dropped while the index exists.
--
-- OPERATOR IMPACT. `batches` is the largest control-plane table. Building the index scans all of it, and
-- Flyway runs each migration inside a transaction, where CONCURRENTLY is not allowed. So this statement
-- holds a SHARE lock on `batches` for the length of a full scan, blocking every commit_file until it
-- finishes. Migrations run in the PostgresControlPlane constructor, synchronously during broker startup,
-- so the scan is startup latency and a failure is a failed start. Measured build rate is about 200k
-- rows/s single-threaded on 2 vCPU: ~6 minutes at 70M rows, on the order of 1.4 hours at the 1e9 rows
-- V24 designs for. At that scale pre-creating the index is required, not optional.
--
-- The statement is idempotent so the build can be taken out of the upgrade path: run the equivalent DDL
-- concurrently BEFORE upgrading, and this migration only ANALYZEs.
--
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS batches_by_timestamp_idx
--       ON batches (topic_id, partition, batch_timestamp(timestamp_type, batch_max_timestamp, log_append_timestamp))
--       INCLUDE (base_offset);
--   ANALYZE batches;
--
-- CREATE INDEX CONCURRENTLY can leave an INVALID index behind if it fails; check pg_index.indisvalid,
-- drop it, and retry before upgrading, because CREATE INDEX IF NOT EXISTS below matches on name only and
-- would keep an invalid index. CREATE INDEX IF NOT EXISTS still opens the table with a SHARE lock before
-- the name check, so even the pre-created case waits for in-flight commits; under the lock_timeout below
-- that can fail the migration and the broker start, and the next start retries it.
--
-- Steady-state cost: a fourth index on the hottest insert table, roughly 4 GB at 70M rows with the
-- INCLUDE column, maintained on every commit_file with one plpgsql evaluation per inserted batch.
--
-- ANALYZE is run here because an expression index is what makes ANALYZE collect statistics on the
-- expression, and a fleet upgrade would otherwise wait for autovacuum on a table where 10% churn can take
-- days. ANALYZE takes SHARE UPDATE EXCLUSIVE and does not block commits.

-- Fail fast rather than queueing for the lock: a pending SHARE request blocks every commit that arrives
-- behind it. On timeout the migration rolls back and the next broker start retries it. The build itself is
-- not capped by this -- lock_timeout covers acquisition only.
SET LOCAL lock_timeout = '5s';

CREATE INDEX IF NOT EXISTS batches_by_timestamp_idx
    ON batches (topic_id, partition, batch_timestamp(timestamp_type, batch_max_timestamp, log_append_timestamp))
    INCLUDE (base_offset);

ANALYZE batches;

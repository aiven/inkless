-- Copyright (c) 2026 Aiven, Helsinki, Finland. https://aiven.io/

-- Index the effective batch timestamp so the timestamp branches of list_offsets_v1 stop scanning the
-- partition.
--
-- MAX_TIMESTAMP and the concrete-timestamp lookup (offsetsForTimes) filter on
-- batch_timestamp(timestamp_type, batch_max_timestamp, log_append_timestamp). No index covers that
-- expression, so both evaluate it for every retained batch of the partition. Cost grows with retention
-- and can exceed statement_timeout. With this index, MAX is a backward range scan, and the
-- concrete-timestamp lookup (rewritten in V31) is a range scan over the batches at or after the target.
-- INCLUDE (base_offset) lets that lookup read the index alone.
--
-- batch_timestamp must stay IMMUTABLE and LANGUAGE plpgsql. The index stores the expression as a
-- function call. If the function becomes LANGUAGE sql, queries inline it to a bare CASE that no longer
-- matches, and the index goes unused without an error.
--
-- OPERATOR IMPACT. Flyway runs this migration in a transaction, where CONCURRENTLY is not allowed. The
-- build holds a SHARE lock on `batches` for a full scan and blocks every commit_file until it ends.
-- Migrations run synchronously at broker start, so the build is startup latency. Measured build rate
-- is about 200k rows/s on 2 vCPU: about 6 minutes at 70M rows, about 1.4 hours at the 1e9 rows V24
-- designs for. At that scale, create the index before the upgrade:
--
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS batches_by_timestamp_idx
--       ON batches (topic_id, partition, batch_timestamp(timestamp_type, batch_max_timestamp, log_append_timestamp))
--       INCLUDE (base_offset);
--   ANALYZE batches;
--
-- A failed CONCURRENTLY build leaves an INVALID index. Check pg_index.indisvalid and drop it before the
-- upgrade, because IF NOT EXISTS matches on name only. The statement below still takes a SHARE lock
-- before the name check, so lock_timeout can fail the broker start when commits are in flight. The next
-- start retries.
--
-- Steady-state cost: a fourth index on the hottest insert table, about 4 GB at 70M rows.
-- ANALYZE runs here so the expression has statistics without waiting for autovacuum.

-- Fail fast instead of queueing: a pending SHARE request blocks every commit behind it. lock_timeout
-- covers lock acquisition only, not the build.

SET LOCAL lock_timeout = '5s';

CREATE INDEX IF NOT EXISTS batches_by_timestamp_idx
    ON batches (topic_id, partition, batch_timestamp(timestamp_type, batch_max_timestamp, log_append_timestamp))
    INCLUDE (base_offset);

ANALYZE batches;

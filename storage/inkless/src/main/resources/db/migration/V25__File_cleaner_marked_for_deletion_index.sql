-- Copyright (c) 2026 Aiven, Helsinki, Finland. https://aiven.io/

-- Index the file cleaner's grace-period predicate so its LIMIT bounds the scan, not just the result.
--
-- files_by_state_only_deleting_idx indexed `state` alone, so `marked_for_deletion_at < ?` was a
-- residual filter: a backlog of deleting rows still inside the grace period was walked in full to
-- return zero rows. Ordering the partial index by marked_for_deletion_at lets the scan stop as soon
-- as the limit is met, and turns the all-within-grace case into a range scan that ends at the first
-- non-matching entry.
--
-- The now-redundant files_by_state_only_deleting_idx is dropped in V26, deliberately not here: Flyway
-- runs one migration per transaction, so a DROP that cannot get its lock would roll back this build
-- with it and the next broker start would redo it.
--
-- Consequence to be aware of: the plan now hands back the oldest eligible files first, where it used
-- to return an arbitrary heap-order prefix. That is an artifact of walking this index, not a contract
-- guarantee (ControlPlane#getFilesToDelete promises no ordering); see FindFilesToDeleteJob.
--
-- OPERATOR IMPACT. The index holds only deleting rows and is therefore small, but building it scans
-- all of `files` to evaluate the predicate, and Flyway runs each migration inside a transaction,
-- where CONCURRENTLY is not allowed. So this statement holds ACCESS EXCLUSIVE on `files` for the
-- length of a full scan, blocking the produce commit path. Migrations run in the PostgresControlPlane
-- constructor, i.e. synchronously during broker startup, so the scan is startup latency and a failure
-- is a failed start.
--
-- The statement is idempotent so the lock can be avoided entirely: run the equivalent DDL
-- concurrently BEFORE upgrading, and this migration becomes a no-op.
--
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS files_by_marked_for_deletion_deleting_idx
--       ON files (marked_for_deletion_at) WHERE state = 'deleting';
--
-- CREATE INDEX CONCURRENTLY can leave an INVALID index behind if it fails; check with
-- `\d files` (or pg_index.indisvalid), drop it, and retry before upgrading, because
-- CREATE INDEX IF NOT EXISTS below matches on name only and would keep an invalid index.
--
-- The migration itself deliberately stays non-concurrent: Flyway rolls it back on failure, so a
-- retry starts clean, whereas a failed concurrent build would leave an invalid index (still
-- maintained on every write) plus a failed history row that blocks every broker start until someone
-- runs flyway repair.
--
-- Note V24__Table_storage_tuning.sql still names files_by_state_only_deleting_idx when explaining why
-- lowering fillfactor on `files` would not help. Its conclusion is unchanged -- marked_for_deletion_at
-- is now the indexed column that mark_file_to_delete_v1 writes, so the update still cannot be HOT.

-- Fail fast rather than queueing for the lock: a pending ACCESS EXCLUSIVE request blocks every
-- commit that arrives behind it, so waiting out a long transaction would stall produce for far longer
-- than the build. On timeout the migration rolls back and the next broker start retries it. The build
-- itself is not capped by this -- lock_timeout covers acquisition only.
SET LOCAL lock_timeout = '5s';

CREATE INDEX IF NOT EXISTS files_by_marked_for_deletion_deleting_idx
    ON files (marked_for_deletion_at) WHERE state = 'deleting';

-- Copyright (c) 2026 Aiven, Helsinki, Finland. https://aiven.io/

-- Drop files_by_state_only_deleting_idx, superseded by files_by_marked_for_deletion_deleting_idx (V25).
--
-- The new index serves `state = 'deleting'` equally well, and the only other query with that predicate
-- (delete_files_v1) is keyed by object_key. Keeping both would cost write amplification on `files` for
-- no read benefit.
--
-- Separate from V25 on purpose. Flyway runs one migration per transaction and migrations run
-- synchronously in the PostgresControlPlane constructor, so pairing this with the CREATE would mean a
-- DROP that cannot get its lock rolls back the index build too, failing broker startup and forcing the
-- next start to redo the scan. On its own, a timeout here costs only a retry of the drop; V25 stays
-- applied and the file cleaner already has the index it needs.
--
-- Operators can also take this out of the upgrade path entirely:
--
--   DROP INDEX CONCURRENTLY IF EXISTS files_by_state_only_deleting_idx;
SET LOCAL lock_timeout = '5s';

DROP INDEX IF EXISTS files_by_state_only_deleting_idx;

-- Copyright (c) 2026 Aiven, Helsinki, Finland. https://aiven.io/

-- Per-table autovacuum and fillfactor settings for the control-plane tables.
--
-- Until now no table carried any storage tuning, so all of them ran on the server defaults. Those
-- defaults assume a table whose dead-tuple rate scales with its size; the control plane has two
-- workloads that both break that assumption, and they need opposite settings.
--
-- What governs the cost of vacuuming a large table is not the number of dead tuples but the number
-- of INDEX PASSES. Vacuum accumulates dead item IDs in autovacuum_work_mem and, each time that
-- fills, scans EVERY index on the table in full. Measured under PG 17's TidStore, an item costs
-- about 5 bytes, so one pass holds roughly (autovacuum_work_mem / 5B) items: ~13M at the 64MB
-- default, ~210M at 1GB. Stock autovacuum_vacuum_scale_factor of 0.2 fires at 0.2 * reltuples, so a
-- 1e9-row batches table triggers at 2e8 dead tuples and needs ~16 complete scans of all three
-- indexes in a single vacuum. That is the mechanism behind multi-hour vacuums, and behind the WAL
-- bursts they push at replicas.
--
-- The trigger is `threshold + scale_factor * reltuples`, a sum, so it cannot express
-- "min(one-pass capacity, a fraction of the table)". Dropping scale_factor to 0 and relying on a
-- large absolute threshold would keep big tables to one pass but would under-vacuum small
-- deployments, where a threshold sized for 1e9 rows is never reached and the table bloats several
-- times over first. The compromise below keeps a small scale_factor so the trigger tracks table
-- size, and picks 0.02 so that the resulting dead-tuple count still fits a single index pass up to
-- ~3.5e9 rows once autovacuum_work_mem is raised to 1GB. For large tables the proportional term
-- dominates and steady-state bloat settles near ~2%; below ~1e6 rows the 10,000-tuple floor takes
-- over and the ratio is higher, which is harmless since absolute dead-tuple counts stay small there.
--
-- autovacuum_work_mem has no per-table equivalent and therefore cannot be set here. Without raising
-- it at the server level the settings below still bound bloat, but large tables fall back to
-- multiple index passes and the main benefit is lost. Note the worst case is
-- autovacuum_work_mem * autovacuum_max_workers, which has to be budgeted against shared_buffers.
--
-- Cost accounting, for why cost_limit is raised on the bulk tables: at cost_delay 2ms and
-- cost_limit 200, the throttle admits ~390 MiB/s of page misses but only ~39 MiB/s of page dirties.
-- Vacuuming a FIFO table dirties nearly every heap page it touches, so the dirty ceiling, not the
-- read ceiling, is what binds.

-- Bulk tables: insert-then-delete, no updates on batches or producer_state, so fillfactor 100 is
-- already correct for them and is left alone. files does take one update (mark_file_to_delete_v1),
-- but it flips `state`, which is indexed by files_by_state_only_deleting_idx; changing an indexed
-- column disqualifies HOT, so lowering fillfactor would not help it either.
ALTER TABLE batches SET (
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_vacuum_threshold = 10000,
    autovacuum_vacuum_insert_scale_factor = 0.02,
    autovacuum_vacuum_insert_threshold = 10000,
    autovacuum_analyze_scale_factor = 0.02,
    autovacuum_analyze_threshold = 10000,
    autovacuum_vacuum_cost_limit = 2000
);

ALTER TABLE files SET (
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_vacuum_threshold = 10000,
    autovacuum_vacuum_insert_scale_factor = 0.02,
    autovacuum_vacuum_insert_threshold = 10000,
    autovacuum_analyze_scale_factor = 0.02,
    autovacuum_analyze_threshold = 10000,
    autovacuum_vacuum_cost_limit = 2000
);

ALTER TABLE producer_state SET (
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_vacuum_threshold = 10000,
    autovacuum_vacuum_insert_scale_factor = 0.02,
    autovacuum_vacuum_insert_threshold = 10000,
    autovacuum_analyze_scale_factor = 0.02,
    autovacuum_analyze_threshold = 10000,
    autovacuum_vacuum_cost_limit = 2000
);

-- logs holds one row per partition and is updated on every commit (commit_file_v1 moves
-- high_watermark and byte_size; delete_records_v1 moves log_start_offset). It carries no index other
-- than logs_pkey and neither update touches (topic_id, partition), so its updates are HOT-eligible
-- and reserved page space keeps them that way instead of migrating them off-page and bloating the
-- primary key.
--
-- The size-proportional trigger is useless here: 0.2 * a few thousand rows lets the table more than
-- double in dead tuples before vacuum runs. An absolute floor and no cost delay keep it clean, which
-- matters because a vacuum on batches can hold an autovacuum worker for hours; if logs does not get
-- a worker in that window it bloats, and every commit_file_v1 and find_batches pays for the lost
-- cache locality on the critical path.
--
-- fillfactor applies to future page fills only. logs is rarely inserted into (one row per partition
-- creation), so existing pages stay as they are and only new partitions benefit. Realising it on an
-- existing deployment needs a rewrite (VACUUM FULL or pg_repack), which is deliberately not done
-- here: it takes an ACCESS EXCLUSIVE lock on a table that is on the produce hot path.
ALTER TABLE logs SET (
    fillfactor = 70,
    autovacuum_vacuum_scale_factor = 0,
    autovacuum_vacuum_threshold = 1000,
    autovacuum_analyze_scale_factor = 0,
    autovacuum_analyze_threshold = 5000,
    autovacuum_vacuum_cost_delay = 0
);

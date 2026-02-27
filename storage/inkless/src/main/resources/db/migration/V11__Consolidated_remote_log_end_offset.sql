-- Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
-- WAL-to-tiered consolidation: track per-partition consolidated tiered end offset.
-- When a local segment is copied to tiered storage, the consolidator calls update_consolidated_tiered_end_offset.
-- WAL files (produce, WRITE_AHEAD_MULTI_SEGMENT) are eligible for deletion when every (topic_id, partition)
-- in that file has max(batches.last_offset) <= logs.consolidated_remote_log_end_offset.

ALTER TABLE logs
    ADD COLUMN IF NOT EXISTS consolidated_remote_log_end_offset offset_nullable_t;

COMMENT ON COLUMN logs.consolidated_remote_log_end_offset IS
    'Highest offset for this partition that has been copied to tiered storage by the consolidation process. NULL if none yet.';

-- Update consolidated tiered end offset for a partition (idempotent; only increases).
CREATE OR REPLACE FUNCTION update_consolidated_tiered_end_offset_v1(
    arg_topic_id topic_id_t,
    arg_partition partition_t,
    arg_end_offset offset_t
)
RETURNS VOID LANGUAGE plpgsql VOLATILE AS $$
BEGIN
    UPDATE logs
    SET consolidated_remote_log_end_offset = GREATEST(COALESCE(consolidated_remote_log_end_offset, -1), arg_end_offset)
    WHERE topic_id = arg_topic_id
      AND partition = arg_partition
      AND (consolidated_remote_log_end_offset IS NULL OR consolidated_remote_log_end_offset < arg_end_offset);
END;
$$;

-- Return file_id and object_key for WAL files (reason=produce, format=1, state=uploaded) that are fully
-- covered by consolidated_remote_log_end_offset for every (topic_id, partition) in that file.
CREATE OR REPLACE FUNCTION get_wal_files_eligible_for_consolidation_deletion_v1()
RETURNS TABLE(file_id BIGINT, object_key object_key_t) LANGUAGE sql STABLE AS $$
    WITH wal_files AS (
        SELECT f.file_id, f.object_key
        FROM files f
        WHERE f.reason = 'produce'
          AND f.format = 1  /* WRITE_AHEAD_MULTI_SEGMENT */
          AND f.state = 'uploaded'
    ),
    file_max_offset_per_partition AS (
        SELECT b.file_id, b.topic_id, b.partition, MAX(b.last_offset) AS max_last_offset
        FROM batches b
        JOIN wal_files w ON b.file_id = w.file_id
        GROUP BY b.file_id, b.topic_id, b.partition
    ),
    files_fully_covered AS (
        SELECT fm.file_id
        FROM file_max_offset_per_partition fm
        JOIN logs l ON fm.topic_id = l.topic_id AND fm.partition = l.partition
        GROUP BY fm.file_id
        HAVING BOOL_AND(l.consolidated_remote_log_end_offset IS NOT NULL
            AND fm.max_last_offset <= l.consolidated_remote_log_end_offset)
    )
    SELECT w.file_id, w.object_key
    FROM wal_files w
    JOIN files_fully_covered f ON w.file_id = f.file_id;
$$;

-- Mark WAL files eligible for consolidation deletion (sets state = 'deleting').
CREATE OR REPLACE FUNCTION mark_wal_files_eligible_for_consolidation_deletion_v1(
    arg_now TIMESTAMP WITH TIME ZONE
)
RETURNS INT LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    affected INT;
BEGIN
    WITH eligible AS (
        SELECT file_id FROM get_wal_files_eligible_for_consolidation_deletion_v1()
    )
    UPDATE files
    SET state = 'deleting',
        marked_for_deletion_at = arg_now
    WHERE file_id IN (SELECT file_id FROM eligible);
    GET DIAGNOSTICS affected = ROW_COUNT;
    RETURN affected;
END;
$$;

-- Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/

ALTER TABLE batches
ALTER CONSTRAINT fk_batches_logs NOT DEFERRABLE;

CREATE OR REPLACE FUNCTION delete_topic_v1(
    arg_now TIMESTAMP WITH TIME ZONE,
    arg_topic_ids UUID[]
)
RETURNS VOID LANGUAGE plpgsql VOLATILE AS $$
BEGIN
    -- Ensure no other transaction commits or does anything else to the affected partitions while this transaction is in progress.
    PERFORM
    FROM logs
    WHERE topic_id = ANY(arg_topic_ids)
    ORDER BY topic_id, partition  -- ordering is important to prevent deadlocks
    FOR UPDATE;

    DROP TABLE IF EXISTS affected_files;
    -- Delete the affected batches and remember what files are affected.
    -- We need to separate deleting batches and finding empty files because if they are in the same requests as CTE,
    -- the query below will see the MVCC snapshot from before deleting batches.
    CREATE TEMPORARY TABLE affected_files
    ON COMMIT DROP
    AS
        WITH deleted_batches AS (
            DELETE FROM batches
            WHERE topic_id = ANY(arg_topic_ids)
            RETURNING file_id
        )
        SELECT file_id
        FROM deleted_batches;

    DELETE FROM logs
    WHERE topic_id = ANY(arg_topic_ids);

    -- Out of the affected files, select those that are now empty (i.e. no batch refers to them)
    -- and mark them for deletion.
    PERFORM mark_file_to_delete_v1(arg_now, file_id)
    FROM (
        SELECT DISTINCT af.file_id
        FROM affected_files AS af
            LEFT JOIN batches AS b ON af.file_id = b.file_id
        WHERE b.batch_id IS NULL
    );
END;
$$
;

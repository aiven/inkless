-- Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/

CREATE OR REPLACE FUNCTION delete_records_v1(
    arg_now TIMESTAMP WITH TIME ZONE,
    arg_requests delete_records_request_v1[]
)
RETURNS SETOF delete_records_response_v1 LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    l_request RECORD;
    l_log RECORD;
    l_converted_offset BIGINT = -1;
    l_deleted_bytes BIGINT;
BEGIN

    DROP TABLE IF EXISTS affected_files;
    CREATE TEMPORARY TABLE affected_files (
        file_id BIGINT PRIMARY KEY
    )
    ON COMMIT DROP;

    FOR l_request IN
        SELECT *
        FROM unnest(arg_requests)
        ORDER BY topic_id, partition  -- ordering is important to prevent deadlocks
    LOOP
        SELECT *
        FROM logs
        WHERE topic_id = l_request.topic_id
            AND partition = l_request.partition
        FOR UPDATE
        INTO l_log;

        IF NOT FOUND THEN
            RETURN NEXT (l_request.topic_id, l_request.partition, 'unknown_topic_or_partition', NULL)::delete_records_response_v1;
            CONTINUE;
        END IF;

        l_converted_offset = CASE
            -- -1 = org.apache.kafka.common.requests.DeleteRecordsRequest.HIGH_WATERMARK
            WHEN l_request.offset = -1 THEN l_log.high_watermark
            ELSE l_request.offset
        END;

        IF l_converted_offset < 0 OR l_converted_offset > l_log.high_watermark THEN
            RETURN NEXT (l_request.topic_id, l_request.partition, 'offset_out_of_range', NULL)::delete_records_response_v1;
            CONTINUE;
        END IF;

        l_converted_offset = GREATEST(l_converted_offset, l_log.log_start_offset);

        -- Delete the affected batches.
        WITH deleted_batches AS (
           DELETE FROM batches
           WHERE topic_id = l_log.topic_id
               AND partition = l_log.partition
               AND last_offset < l_converted_offset
           RETURNING file_id, byte_size
        ),
        -- Remember what files were affected.
        _1 AS (
            INSERT INTO affected_files (file_id)
            SELECT DISTINCT file_id
            FROM deleted_batches
            ON CONFLICT DO NOTHING  -- ignore duplicates
        )
        SELECT COALESCE(SUM(byte_size), 0)
        FROM deleted_batches
        INTO l_deleted_bytes;

        UPDATE logs
        SET log_start_offset = l_converted_offset,
            byte_size = byte_size - l_deleted_bytes
        WHERE topic_id = l_log.topic_id
            AND partition = l_log.partition;

        RETURN NEXT (l_request.topic_id, l_request.partition, NULL, l_converted_offset)::delete_records_response_v1;
    END LOOP;

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

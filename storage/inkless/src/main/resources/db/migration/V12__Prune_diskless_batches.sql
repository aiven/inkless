-- Copyright (c) 2026 Aiven, Helsinki, Finland. https://aiven.io/

CREATE TYPE prune_batches_below_highest_tiered_offset_request_v1 AS (
    topic_id topic_id_t,
    partition partition_t,
    highest_tiered_offset offset_t
);

CREATE TYPE prune_batches_below_highest_tiered_offset_response_v1 AS (
    topic_id topic_id_t,
    partition partition_t,
    log_start_offset offset_t
);

CREATE FUNCTION prune_batches_below_highest_tiered_offset_v1(
    arg_now TIMESTAMP WITH TIME ZONE,
    arg_requests prune_batches_below_highest_tiered_offset_request_v1[]
)
    RETURNS SETOF prune_batches_below_highest_tiered_offset_response_v1 LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    l_request prune_batches_below_highest_tiered_offset_request_v1;
    l_file_id BIGINT;
    l_new_log_start_offset BIGINT;
    l_log_start_offset BIGINT;
    l_high_watermark BIGINT;
BEGIN
    -- lock rows that we need to modify
    IF arg_requests IS NOT NULL AND CARDINALITY(arg_requests) > 0 THEN
        PERFORM 1
        FROM logs l
        WHERE EXISTS(
            SELECT 1
            FROM unnest(arg_requests) AS r
            WHERE r.topic_id = l.topic_id AND r.partition = l.partition
        )
        ORDER BY l.topic_id, l.partition  -- ordering is important to prevent deadlocks
        FOR UPDATE;
        FOREACH l_request IN ARRAY arg_requests LOOP
            FOR l_file_id IN
                WITH deleted AS (
                    DELETE FROM batches
                        WHERE topic_id = l_request.topic_id
                            AND partition = l_request.partition
                            AND last_offset <= l_request.highest_tiered_offset
                        RETURNING file_id
                )
                SELECT DISTINCT file_id FROM deleted
                LOOP
                    -- mark the file for deletion if it doesn't have any more offsets
                    IF NOT EXISTS(SELECT 1 FROM batches WHERE file_id = l_file_id LIMIT 1) THEN
                        PERFORM mark_file_to_delete_v1(arg_now, l_file_id);
                    END IF;
                END LOOP;

            -- select the next smallest batch that is available and use it as the new log_start_offset
            SELECT MIN(base_offset)
            FROM batches
            WHERE topic_id = l_request.topic_id
              AND partition = l_request.partition
            INTO l_new_log_start_offset;
            -- if no row matched, it means that every batch has been tiered and l_new_log_start_offset is NULL
            IF l_new_log_start_offset IS NULL THEN
                SELECT log_start_offset, high_watermark
                FROM logs
                WHERE topic_id = l_request.topic_id AND partition = l_request.partition
                INTO l_log_start_offset, l_high_watermark;
                l_new_log_start_offset := LEAST(l_high_watermark, GREATEST(l_request.highest_tiered_offset + 1, l_log_start_offset));
            END IF;
            -- update the log start offset
            UPDATE logs
            SET log_start_offset = l_new_log_start_offset
            WHERE topic_id = l_request.topic_id AND partition = l_request.partition;

            RETURN NEXT (l_request.topic_id, l_request.partition, l_new_log_start_offset)::prune_batches_below_highest_tiered_offset_response_v1;
        END LOOP;
    END IF;
END;
$$
;
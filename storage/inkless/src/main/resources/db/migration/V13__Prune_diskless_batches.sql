-- Copyright (c) 2026 Aiven, Helsinki, Finland. https://aiven.io/

CREATE TYPE prune_batches_below_highest_tiered_offset_request_v1 AS (
    topic_id topic_id_t,
    partition partition_t,
    highest_tiered_offset offset_t
);

CREATE TYPE prune_batches_below_highest_tiered_offset_error_v1 AS ENUM (
    'none',
    'unknown_topic_or_partition'
);

CREATE TYPE prune_batches_below_highest_tiered_offset_response_v1 AS (
    topic_id topic_id_t,
    partition partition_t,
    log_start_offset offset_nullable_t,
    error prune_batches_below_highest_tiered_offset_error_v1
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
    l_log logs%ROWTYPE;
BEGIN
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
            SELECT *
            FROM logs
            WHERE topic_id = l_request.topic_id
              AND partition = l_request.partition
            ORDER BY topic_id, partition
            FOR UPDATE
            INTO l_log;

            IF NOT FOUND THEN
                RETURN NEXT (
                    l_request.topic_id,
                    l_request.partition,
                    NULL,
                    'unknown_topic_or_partition'::prune_batches_below_highest_tiered_offset_error_v1
                )::prune_batches_below_highest_tiered_offset_response_v1;
                CONTINUE;
            END IF;

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
                    IF NOT EXISTS(SELECT 1 FROM batches WHERE file_id = l_file_id LIMIT 1) THEN
                        PERFORM mark_file_to_delete_v1(arg_now, l_file_id);
                    END IF;
                END LOOP;

            SELECT MIN(base_offset)
            FROM batches
            WHERE topic_id = l_request.topic_id
              AND partition = l_request.partition
            INTO l_new_log_start_offset;

            IF l_new_log_start_offset IS NULL THEN
                l_new_log_start_offset := LEAST(
                    l_log.high_watermark,
                    GREATEST(l_request.highest_tiered_offset + 1, l_log.log_start_offset)
                );
            ELSE
                l_new_log_start_offset := GREATEST(l_log.log_start_offset, l_new_log_start_offset);
            END IF;

            UPDATE logs
            SET log_start_offset = l_new_log_start_offset
            WHERE topic_id = l_request.topic_id AND partition = l_request.partition;

            RETURN NEXT (
                l_request.topic_id,
                l_request.partition,
                l_new_log_start_offset,
                'none'::prune_batches_below_highest_tiered_offset_error_v1
            )::prune_batches_below_highest_tiered_offset_response_v1;
        END LOOP;
    END IF;
END;
$$
;

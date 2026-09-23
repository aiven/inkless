-- Copyright (c) 2026 Aiven, Helsinki, Finland. https://aiven.io/

CREATE OR REPLACE FUNCTION prune_batches_below_highest_tiered_offset_v1(
    arg_now TIMESTAMP WITH TIME ZONE,
    arg_requests prune_batches_below_highest_tiered_offset_request_v1[]
)
    RETURNS SETOF prune_batches_below_highest_tiered_offset_response_v1 LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    l_request prune_batches_below_highest_tiered_offset_request_v1;
    l_deleted_file_id BIGINT;
    l_deleted_file_ids BIGINT[];
    l_deleted_bytes BIGINT;
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
          AND l.deleted_at IS NULL
        ORDER BY l.topic_id, l.partition  -- ordering is important to prevent deadlocks
        FOR UPDATE;

        -- Freeze the pre-prune WAL start as the cross-tier earliest before pruning can advance it.
        -- Preserve an existing value because remote retention owns subsequent forward-only advances.
        UPDATE logs l
        SET remote_log_start_offset = l.log_start_offset
        WHERE l.remote_log_start_offset IS NULL
          AND l.deleted_at IS NULL
          AND EXISTS(
              SELECT 1
              FROM unnest(arg_requests) AS r
              WHERE r.topic_id = l.topic_id AND r.partition = l.partition
          );

        FOREACH l_request IN ARRAY arg_requests LOOP
            SELECT *
            FROM logs
            WHERE topic_id = l_request.topic_id
              AND partition = l_request.partition
              AND deleted_at IS NULL
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

            WITH deleted AS (
                DELETE FROM batches
                    WHERE topic_id = l_request.topic_id
                        AND partition = l_request.partition
                        AND last_offset <= l_request.highest_tiered_offset
                    RETURNING file_id, byte_size
            )
            SELECT COALESCE(SUM(byte_size), 0), COALESCE(ARRAY_AGG(DISTINCT file_id), ARRAY[]::BIGINT[])
            FROM deleted
            INTO l_deleted_bytes, l_deleted_file_ids;

            FOREACH l_deleted_file_id IN ARRAY l_deleted_file_ids LOOP
                IF NOT EXISTS(SELECT 1 FROM batches WHERE file_id = l_deleted_file_id LIMIT 1) THEN
                    PERFORM mark_file_to_delete_v1(arg_now, l_deleted_file_id);
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
            SET log_start_offset = l_new_log_start_offset,
                byte_size = byte_size - l_deleted_bytes,
                -- Recompute only when the prune actually removed batches (the oldest retained batch changed).
                -- The subquery returns NULL when the log is now empty, which is the correct "unknown" state.
                earliest_batch_timestamp = CASE
                    WHEN CARDINALITY(l_deleted_file_ids) > 0 THEN (
                        SELECT batch_timestamp(b.timestamp_type, b.batch_max_timestamp, b.log_append_timestamp)
                        FROM batches b
                        WHERE b.topic_id = l_request.topic_id
                            AND b.partition = l_request.partition
                        ORDER BY b.topic_id, b.partition, b.last_offset
                        LIMIT 1
                    )
                    ELSE earliest_batch_timestamp
                END
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

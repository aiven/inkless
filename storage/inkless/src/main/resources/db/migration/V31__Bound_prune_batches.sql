-- Copyright (c) 2026 Aiven, Helsinki, Finland. https://aiven.io/

CREATE TYPE prune_batches_below_highest_tiered_offset_response_v2 AS (
    topic_id topic_id_t,
    partition partition_t,
    log_start_offset offset_nullable_t,
    error prune_batches_below_highest_tiered_offset_error_v1,
    batches_deleted INT,
    bytes_deleted BIGINT
);

CREATE FUNCTION prune_batches_below_highest_tiered_offset_v2(
    arg_now TIMESTAMP WITH TIME ZONE,
    arg_requests prune_batches_below_highest_tiered_offset_request_v1[],
    max_batches_per_request INT DEFAULT 0
)
    RETURNS SETOF prune_batches_below_highest_tiered_offset_response_v2 LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    l_request RECORD;
    l_log logs%ROWTYPE;
    l_bootstrap_required BOOLEAN;
    l_base_offset_of_first_batch_to_keep offset_nullable_t;
    l_capped_offset offset_nullable_t;
    l_batches_deleted INT;
    l_bytes_deleted BIGINT;
    l_delete_records_response delete_records_response_v1;
BEGIN
    FOR l_request IN
        SELECT *
        FROM unnest(arg_requests)
        ORDER BY topic_id, partition
    LOOP
        SELECT *
        FROM logs
        WHERE topic_id = l_request.topic_id
            AND partition = l_request.partition
            AND deleted_at IS NULL
        INTO l_log;

        IF NOT FOUND THEN
            RETURN NEXT (
                l_request.topic_id,
                l_request.partition,
                NULL,
                'unknown_topic_or_partition'::prune_batches_below_highest_tiered_offset_error_v1,
                NULL,
                NULL
            )::prune_batches_below_highest_tiered_offset_response_v2;
            CONTINUE;
        END IF;

        IF l_log.log_start_offset > l_request.highest_tiered_offset
            AND l_log.remote_log_start_offset IS NOT NULL
        THEN
            RETURN NEXT (
                l_request.topic_id,
                l_request.partition,
                l_log.log_start_offset,
                'none'::prune_batches_below_highest_tiered_offset_error_v1,
                0,
                0::BIGINT
            )::prune_batches_below_highest_tiered_offset_response_v2;
            CONTINUE;
        END IF;

        -- The preceding check only catches a tiered frontier past every remaining batch. The oldest
        -- remaining batch can still straddle it (a multi-record or coalesced batch only partially
        -- tiered), so the log start sitting at or before the tiered frontier doesn't by itself mean
        -- a batch is deletable. Probe the keep-offset unlocked, mirroring retention enforcement's
        -- unlocked boundary scan: commits only append past the tiered frontier and deletes only
        -- advance the log start, so a stale "nothing to delete" read here only defers the delete to
        -- the next pass. Bootstrap still needs the lock even when nothing is deletable, so this only
        -- short-circuits an already-bootstrapped partition.
        IF l_log.remote_log_start_offset IS NOT NULL THEN
            SELECT base_offset
            FROM batches
            WHERE topic_id = l_request.topic_id
                AND partition = l_request.partition
                AND last_offset > l_request.highest_tiered_offset
            ORDER BY topic_id, partition, last_offset
            LIMIT 1
            INTO l_base_offset_of_first_batch_to_keep;

            IF l_base_offset_of_first_batch_to_keep IS NOT NULL
                AND l_base_offset_of_first_batch_to_keep <= l_log.log_start_offset
            THEN
                RETURN NEXT (
                    l_request.topic_id,
                    l_request.partition,
                    l_log.log_start_offset,
                    'none'::prune_batches_below_highest_tiered_offset_error_v1,
                    0,
                    0::BIGINT
                )::prune_batches_below_highest_tiered_offset_response_v2;
                CONTINUE;
            END IF;
        END IF;

        SELECT *
        FROM logs
        WHERE topic_id = l_request.topic_id
            AND partition = l_request.partition
            AND deleted_at IS NULL
        INTO l_log
        FOR UPDATE;

        IF NOT FOUND THEN
            RETURN NEXT (
                l_request.topic_id,
                l_request.partition,
                NULL,
                'unknown_topic_or_partition'::prune_batches_below_highest_tiered_offset_error_v1,
                NULL,
                NULL
            )::prune_batches_below_highest_tiered_offset_response_v2;
            CONTINUE;
        END IF;

        l_bootstrap_required := l_log.remote_log_start_offset IS NULL;

        IF l_bootstrap_required THEN
            UPDATE logs
            SET remote_log_start_offset = l_log.log_start_offset
            WHERE topic_id = l_request.topic_id
                AND partition = l_request.partition;
        END IF;

        -- Batch starts increase with batch ends, so the first index entry past the tiered offset is the
        -- batch to keep. An aggregate over the start column can't walk the covering index in order and
        -- reads the whole untiered tail under the logs row lock.
        SELECT base_offset
        FROM batches
        WHERE topic_id = l_request.topic_id
            AND partition = l_request.partition
            AND last_offset > l_request.highest_tiered_offset
        ORDER BY topic_id, partition, last_offset
        LIMIT 1
        INTO l_base_offset_of_first_batch_to_keep;

        IF l_base_offset_of_first_batch_to_keep IS NULL THEN
            l_base_offset_of_first_batch_to_keep := LEAST(
                l_log.high_watermark,
                l_request.highest_tiered_offset + 1
            );
        END IF;

        IF l_base_offset_of_first_batch_to_keep <= l_log.log_start_offset THEN
            RETURN NEXT (
                l_request.topic_id,
                l_request.partition,
                l_log.log_start_offset,
                'none'::prune_batches_below_highest_tiered_offset_error_v1,
                0,
                0::BIGINT
            )::prune_batches_below_highest_tiered_offset_response_v2;
            CONTINUE;
        END IF;

        IF max_batches_per_request > 0 THEN
            SELECT base_offset
            FROM batches
            WHERE topic_id = l_request.topic_id
                AND partition = l_request.partition
                AND last_offset < l_base_offset_of_first_batch_to_keep
            ORDER BY topic_id, partition, last_offset
            LIMIT 1 OFFSET max_batches_per_request
            INTO l_capped_offset;

            IF l_capped_offset IS NOT NULL THEN
                l_base_offset_of_first_batch_to_keep := l_capped_offset;
            END IF;
        END IF;

        SELECT COUNT(*), COALESCE(SUM(byte_size), 0)
        FROM batches
        WHERE topic_id = l_request.topic_id
            AND partition = l_request.partition
            AND last_offset < l_base_offset_of_first_batch_to_keep
        INTO l_batches_deleted, l_bytes_deleted;

        SELECT *
        FROM delete_records_v1(
            arg_now,
            array[ROW(
                l_request.topic_id,
                l_request.partition,
                l_base_offset_of_first_batch_to_keep
            )::delete_records_request_v1]
        )
        INTO l_delete_records_response;

        IF l_delete_records_response.error IS DISTINCT FROM NULL THEN
            RAISE 'delete_records_v1 returned unexpected error: %', l_delete_records_response;
        END IF;

        RETURN NEXT (
            l_request.topic_id,
            l_request.partition,
            l_delete_records_response.log_start_offset,
            'none'::prune_batches_below_highest_tiered_offset_error_v1,
            COALESCE(l_batches_deleted, 0),
            COALESCE(l_bytes_deleted, 0)
        )::prune_batches_below_highest_tiered_offset_response_v2;
    END LOOP;
END;
$$
;

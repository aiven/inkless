-- Copyright (c) 2026 Aiven, Helsinki, Finland. https://aiven.io/
CREATE OR REPLACE FUNCTION enforce_retention_v2(
    arg_now TIMESTAMP WITH TIME ZONE,
    arg_requests enforce_retention_request_v1[],
    max_batches_per_request INT DEFAULT 0
)
RETURNS SETOF enforce_retention_response_v1 LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    l_request RECORD;
    l_log logs%ROWTYPE;
    l_base_offset_of_first_batch_to_keep offset_nullable_t;
    l_capped_offset offset_nullable_t;
    l_batches_deleted INT;
    l_bytes_deleted BIGINT;
    l_delete_records_response delete_records_response_v1;
BEGIN
    FOR l_request IN
        SELECT *
        FROM unnest(arg_requests)
        ORDER BY topic_id, partition  -- ordering is important to prevent deadlocks
    LOOP
        SELECT *
        FROM logs
        WHERE topic_id = l_request.topic_id
            AND partition = l_request.partition
        INTO l_log; -- NOTE: no FOR UPDATE

        IF NOT FOUND THEN
            RETURN NEXT (
                l_request.topic_id,
                l_request.partition,
                'unknown_topic_or_partition',
                NULL,
                NULL,
                NULL
            )::enforce_retention_response_v1;
            CONTINUE;
        END IF;

        l_base_offset_of_first_batch_to_keep := NULL;

        IF l_request.retention_bytes >= 0
            OR l_request.retention_ms >= 0
        THEN
            -- Short-circuit: if the oldest retained batch (the one at log_start) survives every enabled
            -- policy, nothing is deletable, so skip the O(depth) boundary scan. The oldest batch is summarized
            -- on the log row: its reverse-aggregated size equals the whole log's byte_size, and its effective
            -- timestamp is logs.earliest_batch_timestamp (maintained by V20). earliest_batch_timestamp IS NULL
            -- means "unknown" (lazily populated), so we cannot prove anything and must scan.
            -- Reading these unlocked never over-deletes: the short-circuit only SKIPS work. For time the
            -- decision is stable (commits append newer batches at the head and deletes only make the oldest
            -- batch newer, so a proven "nothing to delete" stays true). For size a concurrent commit can grow
            -- byte_size past retention_bytes right after this read; that only defers the delete to the next
            -- enforcement cycle, exactly like the unlocked boundary scan below, which also decides on a snapshot.
            IF NOT (
                (l_request.retention_bytes < 0 OR l_log.byte_size <= l_request.retention_bytes)
                AND (l_request.retention_ms < 0
                    OR (l_log.earliest_batch_timestamp IS NOT NULL
                        AND l_log.earliest_batch_timestamp >= (EXTRACT(EPOCH FROM arg_now AT TIME ZONE 'UTC') * 1000)::BIGINT - l_request.retention_ms))
            ) THEN
                WITH selected_log AS (
                    SELECT byte_size
                    FROM logs
                    WHERE topic_id = l_request.topic_id
                        AND partition = l_request.partition
                ),
                augmented_batches AS (
                    -- For retention by size:
                    --     Associate with each batch the number of bytes that the log would have if this batch and later batches are retained.
                    --     In other words, this is the reverse aggregated size (counted from the end to the beginning).
                    --     An example:
                    --     Batch size | Aggregated | Reverse aggregated |
                    --     (in order) | size       | size               |
                    --              1 |          1 |   10 -  1 + 1 = 10 |
                    --              2 | 1 + 2 =  3 |   10 -  3 + 2 =  9 |
                    --              3 | 3 + 3 =  6 |   10 -  6 + 3 =  7 |
                    --              4 | 6 + 4 = 10 |   10 - 10 + 4 =  4 |
                    --     The reverse aggregated size is equal to what the aggregated size would be if the sorting order is reverse,
                    --     but doing so explicitly might be costly, hence the formula.
                    -- For retention by time:
                    --     Associate with each batch its effective timestamp.
                    SELECT b.topic_id, b.partition, b.last_offset, b.base_offset,
                        (SELECT byte_size FROM selected_log)
                            - SUM(b.byte_size) OVER (ORDER BY b.topic_id, b.partition, b.last_offset)
                            + b.byte_size
                            AS reverse_agg_byte_size,

                        batch_timestamp(b.timestamp_type, b.batch_max_timestamp, b.log_append_timestamp) AS effective_timestamp
                    FROM batches b
                    WHERE topic_id = l_request.topic_id
                        AND partition = l_request.partition
                    ORDER BY topic_id, partition, last_offset
                )
                -- Look for the first batch that complies with both retention policies (if they are enabled):
                -- For size:
                --    The first batch which being retained with the subsequent batches would make the total log size <= retention_bytes.
                -- For time:
                --    The first batch which effective timestamp is greater or equal to the last timestamp to retain.
                SELECT base_offset
                FROM augmented_batches
                WHERE (l_request.retention_bytes < 0 OR reverse_agg_byte_size <= l_request.retention_bytes)
                    AND (l_request.retention_ms < 0 OR effective_timestamp >= (EXTRACT(EPOCH FROM arg_now AT TIME ZONE 'UTC') * 1000)::BIGINT - l_request.retention_ms)
                ORDER BY topic_id, partition, last_offset
                LIMIT 1
                INTO l_base_offset_of_first_batch_to_keep;

                -- No batch satisfies the retention policy == delete everything, i.e. up to HWM.
                l_base_offset_of_first_batch_to_keep := COALESCE(l_base_offset_of_first_batch_to_keep, l_log.high_watermark);
            END IF;
        END IF;

        -- Nothing to delete (retention disabled, or the oldest batch survives every enabled policy):
        -- report the log_start_offset from the unlocked read above and skip the lock entirely. This is
        -- the steady-state hot path; taking logs FOR UPDATE here would serialize a concurrent commit_file
        -- on the partition for no benefit, since we delete nothing. The reported offset is informational
        -- (trace logging in RetentionEnforcer) and a "nothing to delete" decision cannot be invalidated by
        -- a concurrent commit or delete. If the log was concurrently deleted this reports success with 0
        -- deleted rather than unknown_topic_or_partition, which is harmless (the delete proceeds anyway).
        IF l_base_offset_of_first_batch_to_keep IS NULL THEN
            RETURN NEXT (
                l_request.topic_id,
                l_request.partition,
                NULL,
                0,
                0::BIGINT,
                l_log.log_start_offset
            )::enforce_retention_response_v1;
            CONTINUE;
        END IF;

        -- take the lock only now, for the short delete. The boundary above was computed
        -- without the lock; delete_records_v1 re-clamps to the current log_start, so a concurrent
        -- commit (appends at the head) or delete (advances log_start at the tail) cannot cause
        -- over-deletion.
        SELECT *
        FROM logs
        WHERE topic_id = l_request.topic_id
            AND partition = l_request.partition
        INTO l_log
        FOR UPDATE;

        -- The log may have been deleted between the unlocked scan and here.
        IF NOT FOUND THEN
            RETURN NEXT (
                l_request.topic_id,
                l_request.partition,
                'unknown_topic_or_partition',
                NULL, NULL, NULL
            )::enforce_retention_response_v1;
            CONTINUE;
        END IF;

        -- Enforce max_batches_per_request against the CURRENT rows.
        -- Probe for the (max_batches_per_request + 1)-th deletable batch instead of counting all of them:
        -- OFFSET walks at most that many index entries, so the lock is held for O(max_batches_per_request),
        -- not O(deletable). Counting the whole deletable set here would reintroduce an O(depth) scan under
        -- the lock, defeating the point of computing the boundary unlocked.
        IF max_batches_per_request > 0 THEN
            SELECT base_offset
            FROM batches
            WHERE topic_id = l_request.topic_id
                AND partition = l_request.partition
                AND last_offset < l_base_offset_of_first_batch_to_keep
            ORDER BY topic_id, partition, last_offset
            LIMIT 1 OFFSET max_batches_per_request
            INTO l_capped_offset;

            -- If that batch exists, more than max_batches_per_request would be deleted: cap to its base
            -- offset so exactly max_batches_per_request are removed. Otherwise keep the full boundary.
            IF l_capped_offset IS NOT NULL THEN
                l_base_offset_of_first_batch_to_keep := l_capped_offset;
            END IF;
        END IF;

        -- Recount under the lock so the reported counts match exactly what delete_records_v1 removes.
        SELECT COUNT(*), SUM(byte_size)
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

        -- This should never happen, just fail.
        IF l_delete_records_response.error IS DISTINCT FROM NULL THEN
            RAISE 'delete_records_v1 returned unexpected error: %', l_delete_records_response;
        END IF;

        RETURN NEXT (
            l_request.topic_id,
            l_request.partition,
            NULL::enforce_retention_response_error_v1,
            COALESCE(l_batches_deleted, 0),
            COALESCE(l_bytes_deleted, 0),
            l_delete_records_response.log_start_offset
        )::enforce_retention_response_v1;
    END LOOP;
END;
$$
;

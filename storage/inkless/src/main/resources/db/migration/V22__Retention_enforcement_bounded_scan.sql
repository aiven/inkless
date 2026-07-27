-- Copyright (c) 2026 Aiven, Helsinki, Finland. https://aiven.io/

-- Bounds the retention boundary scan to O(max_batches_per_request) instead of O(depth).
--
-- Problem this fixes (V21 behaviour): when the short-circuit does not fire (something IS deletable),
-- the boundary is computed by a window scan (augmented_batches) that reverse-aggregates byte_size over
-- EVERY deletable batch from log_start forward, then LIMIT 1 picks the first batch to keep. The
-- max_batches_per_request cap was applied only AFTERWARDS (the cap-probe under the lock), so the SCAN
-- was O(deletable) while the DELETE was O(cap). During a bulk drain (retention lowered so almost the
-- whole partition is deletable) that scan is O(partition depth): on multi-million-batch partitions it
-- runs for minutes and the enforcer's JDBC socket.timeout.ms (default 5s) aborts the call, orphaning
-- the still-running backend. The cap never helped because it did not bound the part that was slow.
--
-- Fix: the retention boundary can never be deeper than max_batches_per_request, because the delete is
-- capped there regardless. So scan only the oldest (max_batches_per_request + 1) batches. The extra
-- +1 lets us distinguish "boundary is inside the window" (delete up to it, < cap) from "boundary is
-- deeper than the cap" (no batch in the window complies -> fall through to high_watermark, and the
-- unchanged cap-probe re-clamps to exactly max_batches_per_request). This makes each pass O(cap),
-- index-only on batches_by_last_offset_covering_idx, so a pass can be sized to finish well under
-- socket.timeout.ms. A giant partition then drains over many bounded passes instead of one that
-- never completes.
--
-- max_batches_per_request = 0 (unbounded) keeps the original full-scan behaviour via LIMIT NULL.
--
-- Correctness: unchanged from V21. Retention always deletes oldest-first, so deleting any prefix no
-- longer than the true boundary is safe (it removes a subset of the doomed batches). reverse_agg is
-- still exact within the window: the window holds the oldest k = min(cap+1, depth) batches in offset
-- order, so SUM(byte_size) OVER (ORDER BY last_offset) is the true cumulative size of the first i
-- batches and selected_log.byte_size is the true total. The lock decoupling, the under-lock recount,
-- and delete_records_v1's re-clamp to the current log_start are all identical to V21, so a snapshot-
-- stale boundary can only under-delete, never over-delete. The time-retention assumption (first
-- offset-ordered compliant batch bounds the delete) is exactly the one V21 already made.
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
            -- policy, nothing is deletable, so skip the boundary scan. The oldest batch is summarized
            -- on the log row: its reverse-aggregated size equals the whole log's byte_size, and its
            -- effective timestamp is logs.earliest_batch_timestamp (maintained by V20).
            -- earliest_batch_timestamp IS NULL means "unknown" (lazily populated), so we cannot prove
            -- anything and must scan.
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
                -- Scan only the oldest (max_batches_per_request + 1) batches. The retention boundary
                -- cannot be deeper than max_batches_per_request because the delete is capped there
                -- anyway, so scanning further just to compute a boundary we would immediately cap is
                -- the O(depth) cost being removed. The LIMIT is on the base-table scan, BEFORE the
                -- window, so it rides batches_by_last_offset_covering_idx index-only for O(cap) reads;
                -- placing it above the window would let the running SUM scan the whole partition first.
                -- LIMIT NULL (max_batches_per_request = 0) preserves the original unbounded full scan.
                limited_batches AS (
                    SELECT b.topic_id, b.partition, b.last_offset, b.base_offset, b.byte_size,
                        batch_timestamp(b.timestamp_type, b.batch_max_timestamp, b.log_append_timestamp) AS effective_timestamp
                    FROM batches b
                    WHERE b.topic_id = l_request.topic_id
                        AND b.partition = l_request.partition
                    ORDER BY b.topic_id, b.partition, b.last_offset
                    -- Cast to bigint before +1 so max_batches_per_request = INT_MAX does not overflow int.
                    LIMIT (CASE WHEN max_batches_per_request > 0 THEN max_batches_per_request::bigint + 1 ELSE NULL END)
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
                    --     but doing so explicitly might be costly, hence the formula. It is exact over the window because
                    --     limited_batches holds the oldest batches in offset order, so the running SUM is the true
                    --     cumulative size of the first i batches and (SELECT byte_size FROM selected_log) is the true total.
                    -- For retention by time:
                    --     Associate with each batch its effective timestamp.
                    SELECT topic_id, partition, last_offset, base_offset,
                        (SELECT byte_size FROM selected_log)
                            - SUM(byte_size) OVER (ORDER BY topic_id, partition, last_offset)
                            + byte_size
                            AS reverse_agg_byte_size,
                        effective_timestamp
                    FROM limited_batches
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

                -- No batch in the scanned window complies. Two cases, both correct via high_watermark + cap-probe:
                --   (a) genuine delete-everything: fewer than cap+1 batches exist and none comply == delete up to HWM.
                --   (b) boundary is deeper than the cap: >= cap+1 batches are deletable. high_watermark would delete
                --       all, but the cap-probe below re-clamps to exactly max_batches_per_request, so this pass deletes
                --       cap and the next pass advances. The first cap batches are provably deletable (none of the first
                --       cap+1 complied), so this never over-deletes.
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

-- Copyright (c) 2026 Aiven, Helsinki, Finland. https://aiven.io/
-- Bound find_batches_v2's scan by the fetch budget instead of the read depth (O(result), not O(depth)).
--
-- V15 (LANGUAGE sql) computed ROW_NUMBER()/SUM() OVER windows over every batch in
-- [starting_offset, high_watermark) and only then trimmed to max_partition_fetch_bytes/fetch_max_bytes,
-- so a fetch cost O(high_watermark - starting_offset):
-- a lagging consumer paid for its whole lag on every call.
-- This version streams batches per partition in last_offset order (index scan, no sort),
-- and stops as soon as the per-partition or global byte budget is crossed,
-- so cost is proportional to the bytes returned.
-- Behaviour is otherwise identical to V15: same error handling, always >=1 batch per partition,
-- the budget-crossing batch is included, and responses stay in request order.
CREATE OR REPLACE FUNCTION find_batches_v2(
    arg_requests find_batches_request_v1[],
    fetch_max_bytes INT,
    max_batches_per_partition INT DEFAULT 0
)
RETURNS SETOF find_batches_response_v1 LANGUAGE plpgsql STABLE AS $$
DECLARE
    l_request RECORD;
    l_batch RECORD;
    l_global_bytes BIGINT := 0;
    l_partition_bytes BIGINT;
    l_partition_batch_count BIGINT;
    l_partition_batches batch_info_v1[];
BEGIN
    FOR l_request IN
        SELECT
            r.topic_id,
            r.partition,
            r.starting_offset,
            r.max_partition_fetch_bytes,
            l.log_start_offset,
            l.high_watermark,
            l.topic_name,
            CASE
                WHEN l.topic_id IS NULL THEN 'unknown_topic_or_partition'::find_batches_response_error_v1
                WHEN r.starting_offset < l.log_start_offset OR r.starting_offset > l.high_watermark
                    THEN 'offset_out_of_range'::find_batches_response_error_v1
                ELSE NULL
            END AS error
        FROM unnest(arg_requests) WITH ORDINALITY
            AS r(topic_id, partition, starting_offset, max_partition_fetch_bytes, ordinality)
        LEFT JOIN logs l ON r.topic_id = l.topic_id AND r.partition = l.partition
        ORDER BY r.ordinality
    LOOP
        IF l_request.error IS NOT NULL THEN
            RETURN NEXT (
                l_request.topic_id,
                l_request.partition,
                COALESCE(l_request.log_start_offset, -1)::offset_with_minus_one_t,
                COALESCE(l_request.high_watermark,   -1)::offset_with_minus_one_t,
                NULL,
                l_request.error
            )::find_batches_response_v1;
            CONTINUE;
        END IF;

        l_partition_bytes       := 0;
        l_partition_batch_count := 0;
        l_partition_batches     := '{}'::batch_info_v1[];

        -- Stream this partition's batches in last_offset order and stop at the first budget crossing.
        --
        -- The O(result) win depends on the planner running this as an Index Scan on
        -- batches_by_last_offset_covering_idx (last_offset order, NO Sort) that this loop can abandon early.
        -- That plan is chosen reliably here: EXPLAIN estimates rows=1 for the WHERE predicates (vs 400000+
        -- actual), and believing the scan is tiny the planner always picks the Index Scan + Nested Loop.
        -- The rows=1 misestimate comes from the predicates being on domain-typed columns
        -- (topic_id topic_id_t, partition partition_t, last_offset offset_t): the planner does not derive
        -- useful selectivity through the domains and falls back to a rows=1 estimate. The query does no
        -- explicit casting; this is a property of the column types, not of the predicates.
        -- Keep ORDER BY aligned with that index.
        --
        -- Why the implicit FOR and not an explicit OPEN/FETCH cursor:
        -- an explicit cursor adds cursor_tuple_fraction fast-start planning bias,
        -- but that bias is dormant here, because the rows=1 estimate already forces the plan we want.
        -- It changes nothing today, while its single-row FETCH costs meaningfully more per row
        -- than the implicit loop's batched fetch at large k.
        -- Paying a live cost for a dormant benefit is not worth it.
        --
        -- Reconsider the explicit cursor if the plan shape changes. The trigger is observable in EXPLAIN:
        -- if this query ever plans with a Sort or a materialization step (e.g. after a PG upgrade or a
        -- planner-stats change that makes the row estimate large and accurate), the plan would build the
        -- whole [starting_offset, high_watermark) range before row 1 (silently O(depth) again);
        -- then the fast-start bias becomes worth the cost.
        -- Re-check with EXPLAIN (assert no Sort) on PG upgrades, or if this query or the index changes.
        FOR l_batch IN
            SELECT b.*, f.object_key
            FROM batches b
                JOIN files f ON b.file_id = f.file_id
            WHERE b.topic_id = l_request.topic_id
                AND b.partition = l_request.partition
                AND b.last_offset >= l_request.starting_offset
                AND b.base_offset < l_request.high_watermark
            ORDER BY b.last_offset
        LOOP
            IF l_partition_batch_count = 0
                OR (l_partition_bytes < l_request.max_partition_fetch_bytes
                    AND l_global_bytes < fetch_max_bytes)
            THEN
                -- Appending with || per row is O(k) here, not O(k^2): plpgsql mutates the expanded-array
                -- variable in place (no full copy per append; confirmed by the result-size benchmark).
                -- Do not rewrite to a set-based array_agg; that would forfeit the early EXIT
                -- and rescan the full range.
                l_partition_batches := l_partition_batches || (
                    l_batch.batch_id,
                    l_batch.object_key,
                    (
                        l_batch.magic, l_batch.topic_id, l_request.topic_name, l_batch.partition,
                        l_batch.byte_offset, l_batch.byte_size, l_batch.base_offset, l_batch.last_offset,
                        l_batch.log_append_timestamp, l_batch.batch_max_timestamp, l_batch.timestamp_type
                    )::batch_metadata_v1
                )::batch_info_v1;

                l_partition_bytes := l_partition_bytes + l_batch.byte_size;
                l_global_bytes := l_global_bytes + l_batch.byte_size;
                l_partition_batch_count := l_partition_batch_count + 1;

                EXIT WHEN max_batches_per_partition > 0
                     AND l_partition_batch_count >= max_batches_per_partition;
            ELSE
                EXIT;
            END IF;
        END LOOP;

        RETURN NEXT (
            l_request.topic_id,
            l_request.partition,
            COALESCE(l_request.log_start_offset, -1)::offset_with_minus_one_t,
            COALESCE(l_request.high_watermark,   -1)::offset_with_minus_one_t,
            l_partition_batches,
            NULL
        )::find_batches_response_v1;
    END LOOP;
END;
$$;

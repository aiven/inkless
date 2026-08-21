-- Copyright (c) 2026 Aiven, Helsinki, Finland. https://aiven.io/
-- Make find_batches_v2's minOneMessage allowance request-level, matching upstream Kafka.
--
-- Upstream grants the allowance once per request: ReplicaManager.readFromLocalLog starts minOneMessage
-- true, clears it on the first non-empty read, and clamps every later partition with limitBytes.
-- A partition reached once the budget is spent returns zero bytes.
--
-- Returning zero bytes for a partition that still has data is safe only because callers rotate on it,
-- and none of those callers is visible from here:
--   * the consumer's SubscriptionState (CompletedFetch.drain, bytesRead > 0), which orders its next request;
--   * the broker's fetch session (FetchSession.IncrementalFetchContext.PartitionIterator, recordsSize > 0);
--   * the consolidation and follower fetchers (AbstractFetcherThread.processFetchRequest, validBytes > 0;
--     a partition's first fetch also fires on lag.isEmpty, which costs one round at startup).
-- A partition that returns nothing keeps its place and is served first, with a full budget, next round:
-- fairness is a rotation across responses, not a division within one. Only incremental fetch sessions
-- rotate broker-side; other fetch contexts depend on the client's own rotation, as classic topics do.
--
-- This function does not order arg_requests. It spends the budget in the order given, so the caller has
-- to pass its rotated order, and ReplicaManager.fetchDisklessMessages exists to preserve it. Given an
-- arbitrary but stable order instead, such as a HashMap iteration order, the same partitions win every
-- round and the rest get zero: strictly worse than the per-partition floor this replaced.
--
-- Do not reintroduce a per-partition floor. It makes every partition look served, so every rotation
-- demotes them all, relative order never changes, and the partitions past the budget sit at one batch
-- per round while the head drains. Do not divide the budget between partitions either
-- (fair-share/water-filling): that keeps every partition in every response, and gives a lagging
-- partition 1/n of the budget per round instead of a full budget on its turn.
--
-- Partitions past the budget return an empty batch list, which FetchCompleter maps to MemoryRecords.EMPTY
-- with Errors.NONE and the real high watermark -- the response a caught-up partition already gets.
--
-- One predicate changes, l_partition_batch_count = 0 to l_global_bytes = 0; the function is otherwise
-- identical to V19 -- same error handling, O(result) bounded scan, request ordering, and the
-- max_batches_per_partition cap.
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
            -- l_global_bytes = 0 is the request-level minOneMessage grant: it holds only until the first
            -- batch anywhere in this request is admitted, mirroring upstream clearing the flag on the first
            -- non-empty read. Past that, a partition is served only while both budgets allow, and one that
            -- is served nothing returns an empty list so the caller's rotation serves it first next round.
            IF l_global_bytes = 0
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

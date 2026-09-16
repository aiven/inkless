-- Copyright (c) 2026 Aiven, Helsinki, Finland. https://aiven.io/

-- Make list_offsets_v1's timestamp lookups cost O(batches newer than the target), whatever the planner
-- estimates.
--
-- Both lookups picked one batch with `ORDER BY batch_id LIMIT 1`. batches_pkey yields batch_id order, so
-- the planner has two plans: range-scan batches_by_timestamp_idx (V29) and sort the matches, or walk
-- batches_pkey from the smallest batch_id in the table with the partition and timestamp as filters and
-- stop at the first hit. It picks by estimated selectivity, and that estimate is wrong in two ways that
-- both favor the walk: inside plpgsql the query runs as a generic plan after a few calls, where
-- `>= $1` gets a fixed one-third selectivity; and even a custom plan assumes matches are spread evenly
-- along the walk, while timestamps grow with offset so every match sits at the end. A consumer seeking
-- to a recent time then walks nearly every row, and since the walk is over the whole table's batch_id
-- order, it also crosses every other partition's older rows.
--
-- Rewrite each lookup as MIN(base_offset) over the timestamp predicate, then fetch that batch by offset.
-- No index orders base_offset, so the aggregate has no ordered-walk plan and uses the timestamp index,
-- costing one index entry per batch at or after the target timestamp. That bound relies on V29's
-- INCLUDE (base_offset): without it each matched entry costs a heap fetch and a generic plan may prefer
-- a full partition scan through batches_by_last_offset_covering_idx instead. The follow-up fetch is a
-- point lookup on batches_by_last_offset_covering_idx: batches do not overlap within a partition, so
-- the batch whose base_offset is the minimum is the one with the smallest last_offset >= that minimum.
--
-- The result is unchanged. Within a partition batch_id order is offset order: commit_file_v1 and
-- commit_file_v2 lock the logs row FOR UPDATE, assign offsets from high_watermark, and insert the batch
-- rows in that same transaction, so the first matching batch by batch_id is the one with the smallest
-- base_offset. The rewrite no longer depends on that ordering, so it holds even if the batch_id
-- sequence were ever allocated out of commit order.
--
-- Function replace only: CREATE OR REPLACE FUNCTION takes no table lock.
--
-- This body is the V28 one and reads logs.deleted_at, which exists from V28 on. Applying it by hand to
-- a control plane migrated only through V26 or V27 fails on first execution; a hotfix for those schemas
-- needs the V17 body with the `deleted_at IS NULL` predicate removed and the two lookups rewritten as
-- below.

CREATE OR REPLACE FUNCTION list_offsets_v1(
    arg_requests list_offsets_request_v1[]
)
RETURNS SETOF list_offsets_response_v1 LANGUAGE plpgsql STABLE AS $$
DECLARE
    l_request RECORD;
    l_log RECORD;
    l_max_timestamp BIGINT = NULL;
    l_min_base_offset BIGINT = NULL;
    l_found_timestamp BIGINT = NULL;
    l_found_timestamp_offset BIGINT = NULL;
BEGIN
    FOR l_request IN
        SELECT *
        FROM unnest(arg_requests)
    LOOP
        -- Note that we're not doing locking ("FOR UPDATE") here, as it's not really needed for this read-only function.
        SELECT *
        FROM logs
        WHERE topic_id = l_request.topic_id
            AND partition = l_request.partition
            AND deleted_at IS NULL
        INTO l_log;

        IF NOT FOUND THEN
            -- -1 = org.apache.kafka.common.record.RecordBatch.NO_TIMESTAMP
            RETURN NEXT (l_request.topic_id, l_request.partition, -1, -1, 'unknown_topic_or_partition')::list_offsets_response_v1;
            CONTINUE;
        END IF;

        -- -2 = org.apache.kafka.common.requests.ListOffsetsRequest.EARLIEST_TIMESTAMP
        IF l_request.timestamp = -2 THEN
            -- -1 = org.apache.kafka.common.record.RecordBatch.NO_TIMESTAMP
            RETURN NEXT (l_request.topic_id, l_request.partition, -1, COALESCE(l_log.remote_log_start_offset, l_log.log_start_offset), 'none')::list_offsets_response_v1;
            CONTINUE;
        END IF;

        -- -4 = org.apache.kafka.common.requests.ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP
        IF l_request.timestamp = -4 THEN
            -- -1 = org.apache.kafka.common.record.RecordBatch.NO_TIMESTAMP
            RETURN NEXT (l_request.topic_id, l_request.partition, -1, l_log.log_start_offset, 'none')::list_offsets_response_v1;
            CONTINUE;
        END IF;

        -- -1 = org.apache.kafka.common.requests.ListOffsetsRequest.LATEST_TIMESTAMP
        IF l_request.timestamp = -1 THEN
            -- -1 = org.apache.kafka.common.record.RecordBatch.NO_TIMESTAMP
            RETURN NEXT (l_request.topic_id, l_request.partition, -1, l_log.high_watermark, 'none')::list_offsets_response_v1;
            CONTINUE;
        END IF;

        -- -3 = org.apache.kafka.common.requests.ListOffsetsRequest.MAX_TIMESTAMP
        IF l_request.timestamp = -3 THEN
            SELECT MAX(batch_timestamp(timestamp_type, batch_max_timestamp, log_append_timestamp))
            INTO l_max_timestamp
            FROM batches
            WHERE topic_id = l_request.topic_id
                AND partition = l_request.partition;

            SELECT MIN(base_offset)
            INTO l_min_base_offset
            FROM batches
            WHERE topic_id = l_request.topic_id
                AND partition = l_request.partition
                AND batch_timestamp(timestamp_type, batch_max_timestamp, log_append_timestamp) = l_max_timestamp;

            SELECT last_offset
            INTO l_found_timestamp_offset
            FROM batches
            WHERE topic_id = l_request.topic_id
                AND partition = l_request.partition
                AND last_offset >= l_min_base_offset
            ORDER BY last_offset
            LIMIT 1;

            IF l_found_timestamp_offset IS NULL THEN
                -- -1 = org.apache.kafka.common.record.RecordBatch.NO_TIMESTAMP
                RETURN NEXT (l_request.topic_id, l_request.partition, -1, -1, 'none')::list_offsets_response_v1;
            ELSE
                RETURN NEXT (l_request.topic_id, l_request.partition, l_max_timestamp, l_found_timestamp_offset, 'none')::list_offsets_response_v1;
            END IF;
            CONTINUE;
        END IF;

        -- -5 = org.apache.kafka.common.requests.ListOffsetsRequest.LATEST_TIERED_TIMESTAMP
        IF l_request.timestamp = -5 THEN
            -- -1 = org.apache.kafka.common.record.RecordBatch.NO_TIMESTAMP
            RETURN NEXT (l_request.topic_id, l_request.partition, -1, -1, 'none')::list_offsets_response_v1;
            CONTINUE;
        END IF;

        IF l_request.timestamp < 0 THEN
            -- Unsupported special timestamp.
            -- -1 = org.apache.kafka.common.record.RecordBatch.NO_TIMESTAMP
            RETURN NEXT (l_request.topic_id, l_request.partition, -1, -1, 'unsupported_special_timestamp')::list_offsets_response_v1;
            CONTINUE;
        END IF;

        SELECT MIN(base_offset)
        INTO l_min_base_offset
        FROM batches
        WHERE topic_id = l_request.topic_id
            AND partition = l_request.partition
            AND batch_timestamp(timestamp_type, batch_max_timestamp, log_append_timestamp) >= l_request.timestamp;

        SELECT batch_timestamp(timestamp_type, batch_max_timestamp, log_append_timestamp), base_offset
        INTO l_found_timestamp, l_found_timestamp_offset
        FROM batches
        WHERE topic_id = l_request.topic_id
            AND partition = l_request.partition
            AND last_offset >= l_min_base_offset
        ORDER BY last_offset
        LIMIT 1;

        IF l_found_timestamp_offset IS NULL THEN
            -- -1 = org.apache.kafka.common.record.RecordBatch.NO_TIMESTAMP
            RETURN NEXT (l_request.topic_id, l_request.partition, -1, -1, 'none')::list_offsets_response_v1;
        ELSE
            RETURN NEXT (
                l_request.topic_id, l_request.partition, l_found_timestamp,
                GREATEST(l_found_timestamp_offset, l_log.log_start_offset),
                'none'
            )::list_offsets_response_v1;
        END IF;
        CONTINUE;
    END LOOP;
END;
$$
;

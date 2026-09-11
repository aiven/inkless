-- Copyright (c) 2026 Aiven, Helsinki, Finland. https://aiven.io/

-- Bound the cost of the list_offsets_v1 timestamp lookups to the batches at or after the target.
--
-- Both lookups used `ORDER BY batch_id LIMIT 1`. The planner can satisfy that order by walking
-- batches_pkey from the smallest batch_id in the table, with the partition and timestamp as filters.
-- It chooses the walk because its selectivity estimates assume an early hit, and timestamps grow with
-- offset, so every match is at the end. A seek to a recent time scans nearly every row, including other
-- partitions' rows.
--
-- Each lookup is now MIN(base_offset) over the timestamp predicate, followed by a point lookup of that
-- batch through batches_by_last_offset_covering_idx. No index orders base_offset, so the aggregate uses
-- batches_by_timestamp_idx (V30) and reads one entry per batch at or after the target. Batches do not
-- overlap within a partition, so the batch with the minimum base_offset is the one with the smallest
-- last_offset >= that minimum.
--
-- The result is unchanged: within a partition, commit_file_v1 and commit_file_v2 assign offsets and
-- insert batches under the same logs row lock, so batch_id order is offset order. The new query does not
-- depend on that order.
--
-- Function replace only. CREATE OR REPLACE FUNCTION takes no table lock.
--
-- The body is the V28 one and reads logs.deleted_at. A hand-applied hotfix for a schema older than V28
-- needs the V17 body without the `deleted_at IS NULL` predicate.

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

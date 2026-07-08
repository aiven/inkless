-- Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
--
-- EARLIEST (-2) must reflect the lowest offset readable across all tiers. For consolidating diskless
-- topics the diskless WAL prune frontier (log_start_offset) can be ahead of the remote tier's start,
-- so EARLIEST now prefers remote_log_start_offset when it is known (reported by the partition's
-- classic leader). When it is NULL (pure diskless topics, or before the first leader report -- in
-- which case the WAL has not been pruned yet and log_start_offset still equals the true earliest) we
-- fall back to log_start_offset, preserving the previous behaviour.
--
-- EARLIEST_LOCAL (-4) is unchanged: it always refers to log_start_offset.

CREATE OR REPLACE FUNCTION list_offsets_v1(
    arg_requests list_offsets_request_v1[]
)
RETURNS SETOF list_offsets_response_v1 LANGUAGE plpgsql STABLE AS $$
DECLARE
    l_request RECORD;
    l_log RECORD;
    l_max_timestamp BIGINT = NULL;
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

            SELECT last_offset
            INTO l_found_timestamp_offset
            FROM batches
            WHERE topic_id = l_request.topic_id
                AND partition = l_request.partition
                AND batch_timestamp(timestamp_type, batch_max_timestamp, log_append_timestamp) = l_max_timestamp
            ORDER BY batch_id
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

        SELECT batch_timestamp(timestamp_type, batch_max_timestamp, log_append_timestamp), base_offset
        INTO l_found_timestamp, l_found_timestamp_offset
        FROM batches
        WHERE topic_id = l_request.topic_id
            AND partition = l_request.partition
            AND batch_timestamp(timestamp_type, batch_max_timestamp, log_append_timestamp) >= l_request.timestamp
        ORDER BY batch_id
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

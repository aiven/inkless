-- Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/

CREATE DOMAIN retention_t AS BIGINT NOT NULL
CHECK (VALUE >= -1);

CREATE TYPE enforce_retention_request_v1 AS (
    topic_id topic_id_t,
    partition partition_t,
    retention_bytes retention_t,
    retention_ms retention_t
);

CREATE TYPE enforce_retention_response_error_v1 AS ENUM (
    'unknown_topic_or_partition'
);

CREATE TYPE enforce_retention_response_v1 AS (
    topic_id topic_id_t,
    partition partition_t,
    error enforce_retention_response_error_v1,
    batches_deleted INT,
    bytes_deleted BIGINT,
    log_start_offset offset_nullable_t
);

CREATE FUNCTION enforce_retention_v1(
    arg_now TIMESTAMP WITH TIME ZONE,
    arg_requests enforce_retention_request_v1[]
)
RETURNS SETOF enforce_retention_response_v1 LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    l_request RECORD;
    l_log logs%ROWTYPE;
    l_base_offset_of_first_batch_to_keep offset_nullable_t;
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
        INTO l_log
        FOR UPDATE;

        IF NOT FOUND THEN
            RETURN NEXT (l_request.topic_id, l_request.partition, 'unknown_topic_or_partition', NULL, NULL, NULL)::enforce_retention_response_v1;
            CONTINUE;
        END IF;

        l_base_offset_of_first_batch_to_keep = NULL;

        IF l_request.retention_bytes >= 0 OR l_request.retention_ms >= 0 THEN
            WITH augmented_batches AS (
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
                SELECT topic_id, partition, last_offset,
                    base_offset,
                    l_log.byte_size - SUM(byte_size) OVER (ORDER BY topic_id, partition, last_offset) + byte_size AS reverse_agg_byte_size,
                    batch_timestamp(timestamp_type, batch_max_timestamp, log_append_timestamp) AS effective_timestamp
                FROM batches
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

            -- No batch satisfy the retention policy == delete everything, i.e. up to HWM.
            l_base_offset_of_first_batch_to_keep = COALESCE(l_base_offset_of_first_batch_to_keep, l_log.high_watermark);
        END IF;

        -- Nothing to delete.
        IF l_base_offset_of_first_batch_to_keep IS NULL THEN
            RETURN NEXT (l_request.topic_id, l_request.partition, NULL, 0, 0::BIGINT, l_log.log_start_offset)::enforce_retention_response_v1;
            CONTINUE;
        END IF;

        SELECT COUNT(*), SUM(byte_size)
        FROM batches
        WHERE topic_id = l_request.topic_id
            AND partition = l_request.partition
            AND last_offset < l_base_offset_of_first_batch_to_keep
        INTO l_batches_deleted, l_bytes_deleted;

        SELECT *
        FROM delete_records_v1(arg_now, array[ROW(l_request.topic_id, l_request.partition, l_base_offset_of_first_batch_to_keep)::delete_records_request_v1])
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

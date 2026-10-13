-- Copyright (c) 2026 Aiven, Helsinki, Finland. https://aiven.io/

CREATE TYPE find_batches_request_v2 AS (
    topic_id topic_id_t,
    partition partition_t,
    starting_offset BIGINT,
    max_partition_fetch_bytes INT,
    include_high_watermark_timestamp BOOLEAN
);

CREATE TYPE find_batches_response_v2 AS (
    topic_id topic_id_t,
    partition partition_t,
    log_start_offset offset_with_minus_one_t,
    high_watermark offset_with_minus_one_t,
    high_watermark_timestamp timestamp_nullable_t,
    batches batch_info_v1[],
    error find_batches_response_error_v1
);

CREATE FUNCTION find_batches_v3(
    arg_requests find_batches_request_v2[],
    fetch_max_bytes INT,
    max_batches_per_partition INT DEFAULT 0
)
RETURNS SETOF find_batches_response_v2 LANGUAGE plpgsql STABLE AS $$
DECLARE
    l_request RECORD;
    l_batch RECORD;
    l_global_bytes BIGINT := 0;
    l_partition_bytes BIGINT;
    l_partition_batch_count BIGINT;
    l_partition_batches batch_info_v1[];
    l_head_watermark_timestamp timestamp_nullable_t;
BEGIN
    FOR l_request IN
        SELECT
            r.topic_id,
            r.partition,
            r.starting_offset,
            r.max_partition_fetch_bytes,
            r.include_high_watermark_timestamp,
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
            AS r(topic_id, partition, starting_offset, max_partition_fetch_bytes,
                include_high_watermark_timestamp, ordinality)
        LEFT JOIN logs l ON r.topic_id = l.topic_id AND r.partition = l.partition AND l.deleted_at IS NULL
        ORDER BY r.ordinality
    LOOP
        IF l_request.error IS NOT NULL THEN
            RETURN NEXT (
                l_request.topic_id,
                l_request.partition,
                COALESCE(l_request.log_start_offset, -1)::offset_with_minus_one_t,
                COALESCE(l_request.high_watermark,   -1)::offset_with_minus_one_t,
                NULL::timestamp_nullable_t,
                NULL,
                l_request.error
            )::find_batches_response_v2;
            CONTINUE;
        END IF;

        l_partition_bytes       := 0;
        l_partition_batch_count := 0;
        l_partition_batches     := '{}'::batch_info_v1[];

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
            -- The first admitted batch gets the request-level min-one-message grant. After that,
            -- the partition and request budgets apply to every batch.
            IF l_global_bytes = 0
                OR (l_partition_bytes < l_request.max_partition_fetch_bytes
                    AND l_global_bytes < fetch_max_bytes)
            THEN
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

        l_head_watermark_timestamp := NULL;
        IF l_request.include_high_watermark_timestamp THEN
            SELECT batch_timestamp(
                b.timestamp_type,
                b.batch_max_timestamp,
                b.log_append_timestamp
            )
            INTO l_head_watermark_timestamp
            FROM batches b
            WHERE b.topic_id = l_request.topic_id
                AND b.partition = l_request.partition
                AND b.last_offset < l_request.high_watermark
            ORDER BY b.topic_id, b.partition, b.last_offset DESC
            LIMIT 1;
        END IF;

        RETURN NEXT (
            l_request.topic_id,
            l_request.partition,
            COALESCE(l_request.log_start_offset, -1)::offset_with_minus_one_t,
            COALESCE(l_request.high_watermark,   -1)::offset_with_minus_one_t,
            l_head_watermark_timestamp,
            l_partition_batches,
            NULL
        )::find_batches_response_v2;
    END LOOP;
END;
$$;

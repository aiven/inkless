-- Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/

CREATE DOMAIN topic_id_nullable_t AS UUID;

DROP TYPE find_batches_request_v1 CASCADE;

CREATE TYPE find_batches_request_v1 AS (
    topic_id topic_id_nullable_t,
    topic_name topic_name_t,
    partition partition_t,
    starting_offset BIGINT,
    max_partition_fetch_bytes INT
);

DROP TYPE find_batches_response_v1 CASCADE;

CREATE TYPE find_batches_response_v1 AS (
    topic_id topic_id_t,
    topic_name topic_name_t,
    partition partition_t,
    log_start_offset offset_with_minus_one_t,
    high_watermark offset_with_minus_one_t,
    batches batch_info_v1[],
    error find_batches_response_error_v1
);

CREATE OR REPLACE FUNCTION find_batches_v1(
    arg_requests find_batches_request_v1[],
    fetch_max_bytes INT
)
RETURNS SETOF find_batches_response_v1 LANGUAGE sql STABLE AS $$
    WITH
        requests AS (
            SELECT
                r.topic_id,
                r.topic_name,
                r.partition,
                r.starting_offset,
                r.max_partition_fetch_bytes,
                r.ordinality AS idx -- for preserving original order
            FROM unnest(arg_requests) WITH ORDINALITY AS r(topic_id, topic_name, partition, starting_offset, max_partition_fetch_bytes, ordinality)
        ),
        requests_with_log_info AS (
            SELECT
                r.idx,
                l.topic_id,
                l.topic_name,
                r.topic_id AS requested_topic_id,
                r.topic_name AS requested_topic_name,
                r.partition, r.starting_offset, r.max_partition_fetch_bytes,
                l.log_start_offset, l.high_watermark,
                CASE
                    WHEN l.topic_id IS NULL THEN 'unknown_topic_or_partition'::find_batches_response_error_v1
                    WHEN r.starting_offset < 0 OR r.starting_offset > l.high_watermark THEN 'offset_out_of_range'::find_batches_response_error_v1
                    ELSE NULL
                END AS error
            FROM requests r
            LEFT JOIN logs l ON r.partition = l.partition AND (
                (r.topic_id IS NOT NULL AND r.topic_id = l.topic_id) OR
                (r.topic_id IS NULL AND r.topic_name = l.topic_name)
            )
        ),
        all_batches_with_metadata AS (
            SELECT
                r.idx,
                (
                    b.batch_id,
                    f.object_key,
                    (
                        b.magic, b.topic_id, r.topic_name, b.partition, b.byte_offset, b.byte_size,
                        b.base_offset, b.last_offset, b.log_append_timestamp, b.batch_max_timestamp,
                        b.timestamp_type
                    )::batch_metadata_v1
                )::batch_info_v1 AS batch_data,
                b.byte_size, b.base_offset, r.max_partition_fetch_bytes,
                ROW_NUMBER() OVER (PARTITION BY r.idx ORDER BY b.base_offset) as rn,
                SUM(b.byte_size) OVER (PARTITION BY r.idx ORDER BY b.base_offset) as partition_cumulative_bytes
            FROM requests_with_log_info r
            JOIN batches b ON r.topic_id = b.topic_id AND r.partition = b.partition
            JOIN files f ON b.file_id = f.file_id
            WHERE r.error IS NULL
                AND b.last_offset >= r.starting_offset
                AND b.base_offset < r.high_watermark
        ),
        per_partition_limited_batches AS (
            SELECT idx, batch_data, byte_size, base_offset, rn
            FROM all_batches_with_metadata
            WHERE rn = 1  -- each partition gets always at least one batch
                -- include also last batch, even if it overflows max.partition.fetch.bytes
                OR (partition_cumulative_bytes - byte_size) < max_partition_fetch_bytes
        ),
        final_batch_set AS (
            SELECT idx, batch_data, base_offset, rn
            FROM (
                SELECT *, SUM(byte_size) OVER (ORDER BY idx, base_offset) as global_cumulative_bytes
                FROM per_partition_limited_batches
            ) AS sized_batches
            WHERE rn = 1 OR  -- each partition gets always at least one batch
                -- include also last batch, even if it overflows fetch.max.bytes
                (global_cumulative_bytes - byte_size) < fetch_max_bytes
        ),
        aggregated_batches AS (
            SELECT
                idx,
                array_agg(batch_data ORDER BY base_offset) AS batches
            FROM final_batch_set
            GROUP BY idx
        )
    SELECT
        COALESCE(r.topic_id, r.requested_topic_id),
        COALESCE(r.topic_name, r.requested_topic_name),
        r.partition,
        COALESCE(r.log_start_offset, -1),
        COALESCE(r.high_watermark, -1),
        CASE WHEN r.error IS NULL THEN COALESCE(ab.batches, '{}'::batch_info_v1[]) ELSE NULL END,
        r.error
    FROM requests_with_log_info r
    LEFT JOIN aggregated_batches ab ON r.idx = ab.idx
    ORDER BY r.idx;
$$;

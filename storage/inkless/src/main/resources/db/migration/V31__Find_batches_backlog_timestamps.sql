-- Copyright (c) 2026 Aiven, Helsinki, Finland. https://aiven.io/

-- V31 adds rolling-upgrade-safe V2 request and response types for find_batches_v3.
-- V3 preserves the request-level budget from V27 and the soft-delete behavior from V28.
-- See V27 and V28 for the full planner rationale.
-- The following boundary lookups use the `batches_by_last_offset_covering_idx` access path. Re-check both
-- boundary lookups with EXPLAIN after a PostgreSQL upgrade.

CREATE TABLE batch_timestamp_boundaries (
    batch_id BIGINT PRIMARY KEY REFERENCES batches(batch_id) ON DELETE CASCADE,
    first_batch_timestamp timestamp_t,
    last_batch_timestamp timestamp_t
);

-- Stores the effective timestamps of the physical batches at each end of a coalesced row.
-- Rows written before V31 leave these columns null, and readers fall back to the row timestamp.
CREATE FUNCTION flush_commit_run_v3(
    arg_file_id BIGINT,
    arg_magic magic_t,
    arg_topic_id topic_id_t,
    arg_partition partition_t,
    arg_base_offset offset_t,
    arg_last_offset offset_t,
    arg_byte_offset byte_offset_t,
    arg_byte_size byte_size_t,
    arg_timestamp_type timestamp_type_t,
    arg_log_append_timestamp timestamp_t,
    arg_batch_max_timestamp timestamp_t,
    arg_first_batch_timestamp timestamp_t,
    arg_last_batch_timestamp timestamp_t
)
RETURNS VOID LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    l_batch_id BIGINT;
BEGIN
    INSERT INTO batches (
        magic,
        topic_id, partition,
        base_offset,
        last_offset,
        file_id,
        byte_offset, byte_size,
        timestamp_type, log_append_timestamp, batch_max_timestamp
    )
    VALUES (
        arg_magic,
        arg_topic_id, arg_partition,
        arg_base_offset,
        arg_last_offset,
        arg_file_id,
        arg_byte_offset, arg_byte_size,
        arg_timestamp_type,
        arg_log_append_timestamp,
        arg_batch_max_timestamp
    )
    RETURNING batch_id
    INTO l_batch_id;

    INSERT INTO batch_timestamp_boundaries (
        batch_id, first_batch_timestamp, last_batch_timestamp
    )
    VALUES (
        l_batch_id,
        arg_first_batch_timestamp,
        arg_last_batch_timestamp
    );
END;
$$;

-- Preserve the first and last physical batch timestamps while coalescing.
CREATE OR REPLACE FUNCTION commit_file_v2(
    arg_object_key object_key_t,
    arg_format format_t,
    arg_uploader_broker_id broker_id_t,
    arg_file_size byte_size_t,
    arg_now TIMESTAMP WITH TIME ZONE,
    arg_requests commit_batch_request_v1[]
)
RETURNS SETOF commit_batch_response_v1 LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    l_new_file_id BIGINT;
    l_request RECORD;
    l_log logs%ROWTYPE;
    l_duplicate RECORD;
    l_assigned_offset offset_nullable_t;
    l_new_high_watermark offset_nullable_t;
    l_last_sequence_in_producer_epoch BIGINT;
    l_log_append_timestamp BIGINT;
    l_request_timestamp BIGINT;

    l_run_open BOOLEAN := FALSE;
    l_run_topic_id UUID;
    l_run_partition INT;
    l_run_magic SMALLINT;
    l_run_timestamp_type SMALLINT;
    l_run_base_offset BIGINT;
    l_run_last_offset BIGINT;
    l_run_byte_offset BIGINT;
    l_run_byte_size BIGINT;
    l_run_max_timestamp BIGINT;
    l_run_first_timestamp BIGINT;
    l_run_last_timestamp BIGINT;
BEGIN
    l_log_append_timestamp := (EXTRACT(EPOCH FROM arg_now AT TIME ZONE 'UTC') * 1000)::BIGINT;

    INSERT INTO files (object_key, format, reason, state, uploader_broker_id, committed_at, size)
    VALUES (arg_object_key, arg_format, 'produce', 'uploaded', arg_uploader_broker_id, arg_now, arg_file_size)
    RETURNING file_id
    INTO l_new_file_id;

    DROP TABLE IF EXISTS logs_tmp;
    CREATE TEMPORARY TABLE logs_tmp
    ON COMMIT DROP
    AS
        SELECT *
        FROM logs
        WHERE (topic_id, partition) IN (SELECT DISTINCT topic_id, partition FROM unnest(arg_requests))
            AND deleted_at IS NULL
        ORDER BY topic_id, partition
        FOR UPDATE;

    FOR l_request IN
        SELECT *
        FROM unnest(arg_requests)
    LOOP
        IF l_log.topic_id IS DISTINCT FROM l_request.topic_id
            OR l_log.partition IS DISTINCT FROM l_request.partition THEN

            SELECT *
            FROM logs_tmp
            WHERE topic_id = l_request.topic_id
                AND partition = l_request.partition
            INTO l_log;

            IF NOT FOUND THEN
                RETURN NEXT (l_request.topic_id, l_request.partition, NULL, NULL, -1, 'nonexistent_log')::commit_batch_response_v1;
                CONTINUE;
            END IF;
        END IF;

        l_assigned_offset = l_log.high_watermark;

        IF l_request.producer_id > -1 AND l_request.producer_epoch > -1
        THEN
             IF EXISTS (
                SELECT 1
                FROM producer_state
                WHERE topic_id = l_request.topic_id
                    AND partition = l_request.partition
                    AND producer_id = l_request.producer_id
                    AND producer_epoch > l_request.producer_epoch
             ) THEN
                RETURN NEXT (l_request.topic_id, l_request.partition, NULL, NULL, -1, 'invalid_producer_epoch')::commit_batch_response_v1;
                CONTINUE;
             END IF;

             SELECT MAX(last_sequence)
             INTO l_last_sequence_in_producer_epoch
             FROM producer_state
             WHERE topic_id = l_request.topic_id
                 AND partition = l_request.partition
                 AND producer_id = l_request.producer_id
                 AND producer_epoch = l_request.producer_epoch;

            IF l_last_sequence_in_producer_epoch IS NULL THEN
                IF l_request.base_sequence <> 0
                THEN
                    RETURN NEXT (l_request.topic_id, l_request.partition, NULL, NULL, -1, 'sequence_out_of_order')::commit_batch_response_v1;
                    CONTINUE;
                END IF;
            ELSE
                SELECT *
                FROM producer_state
                WHERE topic_id = l_request.topic_id
                    AND partition = l_request.partition
                    AND producer_id = l_request.producer_id
                    AND producer_epoch = l_request.producer_epoch
                    AND base_sequence = l_request.base_sequence
                    AND last_sequence = l_request.last_sequence
                INTO l_duplicate;
                IF FOUND THEN
                    RETURN NEXT (l_request.topic_id, l_request.partition, l_log.log_start_offset, l_duplicate.assigned_offset, l_duplicate.batch_max_timestamp, 'duplicate_batch')::commit_batch_response_v1;
                    CONTINUE;
                END IF;

                IF (l_request.base_sequence - 1) <> l_last_sequence_in_producer_epoch OR (l_last_sequence_in_producer_epoch = 2147483647 AND l_request.base_sequence <> 0) THEN
                    RETURN NEXT (l_request.topic_id, l_request.partition, NULL, NULL, -1, 'sequence_out_of_order')::commit_batch_response_v1;
                    CONTINUE;
                END IF;
            END IF;

            INSERT INTO producer_state (
                topic_id, partition, producer_id,
                producer_epoch, base_sequence, last_sequence, assigned_offset, batch_max_timestamp
            )
            VALUES (
                l_request.topic_id, l_request.partition, l_request.producer_id,
                l_request.producer_epoch, l_request.base_sequence, l_request.last_sequence, l_assigned_offset, l_request.batch_max_timestamp
            );
            DELETE FROM producer_state
            WHERE topic_id = l_request.topic_id
                AND partition = l_request.partition
                AND producer_id = l_request.producer_id
                AND row_id <= (
                    SELECT row_id
                    FROM producer_state
                    WHERE topic_id = l_request.topic_id
                        AND partition = l_request.partition
                        AND producer_id = l_request.producer_id
                    ORDER BY row_id DESC
                    LIMIT 1
                    OFFSET 5
                );
        END IF;

        UPDATE logs_tmp
        SET high_watermark = high_watermark + (l_request.last_offset - l_request.base_offset + 1),
            byte_size = byte_size + l_request.byte_size
        WHERE topic_id = l_request.topic_id
            AND partition = l_request.partition
        RETURNING high_watermark
        INTO l_new_high_watermark;

        l_log.high_watermark = l_new_high_watermark;
        l_request_timestamp := CASE l_request.timestamp_type
            WHEN 1 THEN l_log_append_timestamp
            ELSE l_request.batch_max_timestamp
        END;

        IF l_run_open
            AND l_run_topic_id = l_request.topic_id
            AND l_run_partition = l_request.partition
            AND l_run_magic = l_request.magic
            AND l_run_timestamp_type = l_request.timestamp_type
            AND l_request.byte_offset = l_run_byte_offset + l_run_byte_size
        THEN
            l_run_last_offset := l_new_high_watermark - 1;
            l_run_byte_size := l_run_byte_size + l_request.byte_size;
            l_run_max_timestamp := GREATEST(l_run_max_timestamp, l_request.batch_max_timestamp);
            l_run_last_timestamp := l_request_timestamp;
        ELSE
            IF l_run_open THEN
                PERFORM flush_commit_run_v3(
                    l_new_file_id, l_run_magic, l_run_topic_id, l_run_partition,
                    l_run_base_offset, l_run_last_offset, l_run_byte_offset, l_run_byte_size,
                    l_run_timestamp_type, l_log_append_timestamp, l_run_max_timestamp,
                    l_run_first_timestamp, l_run_last_timestamp);
            END IF;
            l_run_open := TRUE;
            l_run_topic_id := l_request.topic_id;
            l_run_partition := l_request.partition;
            l_run_magic := l_request.magic;
            l_run_timestamp_type := l_request.timestamp_type;
            l_run_base_offset := l_assigned_offset;
            l_run_last_offset := l_new_high_watermark - 1;
            l_run_byte_offset := l_request.byte_offset;
            l_run_byte_size := l_request.byte_size;
            l_run_max_timestamp := l_request.batch_max_timestamp;
            l_run_first_timestamp := l_request_timestamp;
            l_run_last_timestamp := l_request_timestamp;
        END IF;

        RETURN NEXT (l_request.topic_id, l_request.partition, l_log.log_start_offset, l_assigned_offset, l_request.batch_max_timestamp, 'none')::commit_batch_response_v1;
    END LOOP;

    IF l_run_open THEN
        PERFORM flush_commit_run_v3(
            l_new_file_id, l_run_magic, l_run_topic_id, l_run_partition,
            l_run_base_offset, l_run_last_offset, l_run_byte_offset, l_run_byte_size,
            l_run_timestamp_type, l_log_append_timestamp, l_run_max_timestamp,
            l_run_first_timestamp, l_run_last_timestamp);
    END IF;

    UPDATE logs
    SET high_watermark = logs_tmp.high_watermark,
        byte_size = logs_tmp.byte_size
    FROM logs_tmp
    WHERE logs.topic_id = logs_tmp.topic_id
        AND logs.partition = logs_tmp.partition;

    UPDATE logs l
    SET earliest_batch_timestamp = (
        SELECT batch_timestamp(b.timestamp_type, b.batch_max_timestamp, b.log_append_timestamp)
        FROM batches b
        WHERE b.topic_id = l.topic_id
            AND b.partition = l.partition
        ORDER BY b.topic_id, b.partition, b.last_offset
        LIMIT 1
    )
    WHERE (l.topic_id, l.partition) IN (SELECT DISTINCT topic_id, partition FROM unnest(arg_requests))
        AND l.earliest_batch_timestamp IS NULL
        AND l.deleted_at IS NULL;

    IF NOT EXISTS (SELECT 1 FROM batches WHERE file_id = l_new_file_id LIMIT 1) THEN
        PERFORM mark_file_to_delete_v1(arg_now, l_new_file_id);
    END IF;
END;
$$;

CREATE TYPE find_batches_request_v2 AS (
    topic_id topic_id_t,
    partition partition_t,
    starting_offset BIGINT,
    max_partition_fetch_bytes INT,
    include_backlog_timestamps BOOLEAN
);

CREATE TYPE find_batches_response_v2 AS (
    topic_id topic_id_t,
    partition partition_t,
    log_start_offset offset_with_minus_one_t,
    high_watermark offset_with_minus_one_t,
    starting_offset_timestamp timestamp_nullable_t,
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
    l_starting_offset_timestamp timestamp_nullable_t;
    l_high_watermark_timestamp timestamp_nullable_t;
    l_budget_rejected BOOLEAN;
    l_batch_cap_reached BOOLEAN;
    l_last_returned_last_offset BIGINT;
BEGIN
    FOR l_request IN
        SELECT
            r.topic_id,
            r.partition,
            r.starting_offset,
            r.max_partition_fetch_bytes,
            r.include_backlog_timestamps,
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
                include_backlog_timestamps, ordinality)
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
                NULL::timestamp_nullable_t,
                NULL,
                l_request.error
            )::find_batches_response_v2;
            CONTINUE;
        END IF;

        l_partition_bytes       := 0;
        l_partition_batch_count := 0;
        l_partition_batches     := '{}'::batch_info_v1[];
        l_starting_offset_timestamp := NULL;
        l_high_watermark_timestamp := NULL;
        l_budget_rejected := FALSE;
        l_batch_cap_reached := FALSE;
        l_last_returned_last_offset := NULL;

        -- The order must stay aligned with batches_by_last_offset_covering_idx, with no Sort node.
        FOR l_batch IN
            SELECT b.*, f.object_key, t.first_batch_timestamp, t.last_batch_timestamp
            FROM batches b
                JOIN files f ON b.file_id = f.file_id
                LEFT JOIN batch_timestamp_boundaries t ON b.batch_id = t.batch_id
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
                l_last_returned_last_offset := l_batch.last_offset;

                IF max_batches_per_partition > 0
                    AND l_partition_batch_count >= max_batches_per_partition
                THEN
                    l_batch_cap_reached := TRUE;
                    EXIT;
                END IF;
            ELSE
                -- Keep the early EXIT. Replacing it with array_agg scans past the budget boundary.
                IF l_request.include_backlog_timestamps THEN
                    l_starting_offset_timestamp := COALESCE(
                        l_batch.first_batch_timestamp,
                        batch_timestamp(
                            l_batch.timestamp_type,
                            l_batch.batch_max_timestamp,
                            l_batch.log_append_timestamp
                        )
                    )::bigint;
                END IF;
                l_budget_rejected := TRUE;
                EXIT;
            END IF;
        END LOOP;

        IF l_request.include_backlog_timestamps AND l_batch_cap_reached THEN
            SELECT COALESCE(
                t.first_batch_timestamp,
                batch_timestamp(
                    b.timestamp_type,
                    b.batch_max_timestamp,
                    b.log_append_timestamp
                )
            )
            INTO l_starting_offset_timestamp
            FROM batches b
                LEFT JOIN batch_timestamp_boundaries t ON b.batch_id = t.batch_id
            WHERE b.topic_id = l_request.topic_id
                AND b.partition = l_request.partition
                AND b.last_offset > l_last_returned_last_offset
                AND b.base_offset < l_request.high_watermark
            ORDER BY b.last_offset ASC
            LIMIT 1;
        ELSIF l_request.include_backlog_timestamps
            AND l_partition_batch_count = 0
            AND NOT l_budget_rejected
        THEN
            SELECT COALESCE(
                t.first_batch_timestamp,
                batch_timestamp(
                    b.timestamp_type,
                    b.batch_max_timestamp,
                    b.log_append_timestamp
                )
            )
            INTO l_starting_offset_timestamp
            FROM batches b
                LEFT JOIN batch_timestamp_boundaries t ON b.batch_id = t.batch_id
            WHERE b.topic_id = l_request.topic_id
                AND b.partition = l_request.partition
                AND b.last_offset >= l_request.starting_offset
                AND b.base_offset < l_request.high_watermark
            ORDER BY b.last_offset ASC
            LIMIT 1;
        END IF;

        IF l_request.include_backlog_timestamps THEN
            SELECT COALESCE(
                t.last_batch_timestamp,
                batch_timestamp(
                    b.timestamp_type,
                    b.batch_max_timestamp,
                    b.log_append_timestamp
                )
            )
            INTO l_high_watermark_timestamp
            FROM batches b
                LEFT JOIN batch_timestamp_boundaries t ON b.batch_id = t.batch_id
            WHERE b.topic_id = l_request.topic_id
                AND b.partition = l_request.partition
                AND b.last_offset < l_request.high_watermark
            ORDER BY b.last_offset DESC
            LIMIT 1;
        END IF;

        RETURN NEXT (
            l_request.topic_id,
            l_request.partition,
            COALESCE(l_request.log_start_offset, -1)::offset_with_minus_one_t,
            COALESCE(l_request.high_watermark,   -1)::offset_with_minus_one_t,
            l_starting_offset_timestamp,
            l_high_watermark_timestamp,
            l_partition_batches,
            NULL
        )::find_batches_response_v2;
    END LOOP;
END;
$$;

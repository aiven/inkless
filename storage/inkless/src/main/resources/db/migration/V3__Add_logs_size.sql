-- Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/

-- 1. Introduce the `byte_size` column.

ALTER TABLE logs
ADD COLUMN byte_size byte_size_t DEFAULT 0;

UPDATE logs
SET byte_size = aggregated.total_byte_size
FROM (
    SELECT topic_id, partition, SUM(byte_size) AS total_byte_size
    FROM batches
    GROUP BY topic_id, partition
) AS aggregated
WHERE logs.topic_id = aggregated.topic_id
    AND logs.partition = aggregated.partition;

-- 2. Update functions to support `byte_size`.

CREATE OR REPLACE FUNCTION commit_file_v1(
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
BEGIN
    INSERT INTO files (object_key, format, reason, state, uploader_broker_id, committed_at, size)
    VALUES (arg_object_key, arg_format, 'produce', 'uploaded', arg_uploader_broker_id, arg_now, arg_file_size)
    RETURNING file_id
    INTO l_new_file_id;

    -- We use this temporary table to perform the write operations in loop on it first
    -- and only then dump the result on the real table. This reduces the WAL pressure and latency of the function.
    DROP TABLE IF EXISTS logs_tmp;
    CREATE TEMPORARY TABLE logs_tmp
    ON COMMIT DROP
    AS
        -- Extract the relevant logs into the temporary table and simultaneously lock them.
        -- topic_name and log_start_offset aren't technically needed, but having them allows declaring `l_log logs%ROWTYPE`.
        SELECT *
        FROM logs
        WHERE (topic_id, partition) IN (SELECT DISTINCT topic_id, partition FROM unnest(arg_requests))
        ORDER BY topic_id, partition  -- ordering is important to prevent deadlocks
        FOR UPDATE;

    FOR l_request IN
        SELECT *
        FROM unnest(arg_requests)
    LOOP
        -- A small optimization: select the log into a variable only if it's a different topic-partition.
        -- Batches are sorted by topic-partitions, so this makes sense.
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

        -- Validate that the new request base sequence is not larger than the previous batch last sequence
        IF l_request.producer_id > -1 AND l_request.producer_epoch > -1
        THEN
            -- If there are previous batches for the producer, check that the producer epoch is not smaller than the last batch
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

            -- If there are previous batches for the producer
            IF l_last_sequence_in_producer_epoch IS NULL THEN
                -- If there are no previous batches for the producer, the base sequence must be 0
                IF l_request.base_sequence <> 0
                THEN
                    RETURN NEXT (l_request.topic_id, l_request.partition, NULL, NULL, -1, 'sequence_out_of_order')::commit_batch_response_v1;
                    CONTINUE;
                END IF;
            ELSE
                -- Check for duplicates
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

                -- Check that the sequence is not out of order.
                -- A sequence is out of order if the base sequence is not a continuation of the last sequence
                -- or, in case of wraparound, the base sequence must be 0 and the last sequence must be 2147483647 (Integer.MAX_VALUE).
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
            -- Keep only the last 5 records.
            -- 5 == org.apache.kafka.storage.internals.log.ProducerStateEntry.NUM_BATCHES_TO_RETAIN
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
            l_request.magic,
            l_request.topic_id, l_request.partition,
            l_assigned_offset,
            l_new_high_watermark - 1,
            l_new_file_id,
            l_request.byte_offset, l_request.byte_size,
            l_request.timestamp_type,
            (EXTRACT(EPOCH FROM arg_now AT TIME ZONE 'UTC') * 1000)::BIGINT,
            l_request.batch_max_timestamp
        );

        RETURN NEXT (l_request.topic_id, l_request.partition, l_log.log_start_offset, l_assigned_offset, l_request.batch_max_timestamp, 'none')::commit_batch_response_v1;
    END LOOP;

    -- Transfer from the temporary to real table.
    UPDATE logs
    SET high_watermark = logs_tmp.high_watermark,
        byte_size = logs_tmp.byte_size
    FROM logs_tmp
    WHERE logs.topic_id = logs_tmp.topic_id
        AND logs.partition = logs_tmp.partition;

    IF NOT EXISTS (SELECT 1 FROM batches WHERE file_id = l_new_file_id LIMIT 1) THEN
        PERFORM mark_file_to_delete_v1(arg_now, l_new_file_id);
    END IF;
END;
$$
;


CREATE OR REPLACE FUNCTION delete_records_v1(
    arg_now TIMESTAMP WITH TIME ZONE,
    arg_requests delete_records_request_v1[]
)
RETURNS SETOF delete_records_response_v1 LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    l_request RECORD;
    l_log RECORD;
    l_converted_offset BIGINT = -1;
    l_deleted_bytes BIGINT;
BEGIN

    DROP TABLE IF EXISTS affected_files;
    CREATE TEMPORARY TABLE affected_files (
        file_id BIGINT PRIMARY KEY
    )
    ON COMMIT DROP;

    FOR l_request IN
        SELECT *
        FROM unnest(arg_requests)
    LOOP
        SELECT *
        FROM logs
        WHERE topic_id = l_request.topic_id
            AND partition = l_request.partition
        ORDER BY topic_id, partition  -- ordering is important to prevent deadlocks
        FOR UPDATE
        INTO l_log;

        IF NOT FOUND THEN
            RETURN NEXT (l_request.topic_id, l_request.partition, 'unknown_topic_or_partition', NULL)::delete_records_response_v1;
            CONTINUE;
        END IF;

        l_converted_offset = CASE
            -- -1 = org.apache.kafka.common.requests.DeleteRecordsRequest.HIGH_WATERMARK
            WHEN l_request.offset = -1 THEN l_log.high_watermark
            ELSE l_request.offset
        END;

        IF l_converted_offset < 0 OR l_converted_offset > l_log.high_watermark THEN
            RETURN NEXT (l_request.topic_id, l_request.partition, 'offset_out_of_range', NULL)::delete_records_response_v1;
            CONTINUE;
        END IF;

        l_converted_offset = GREATEST(l_converted_offset, l_log.log_start_offset);

        -- Delete the affected batches.
        WITH deleted_batches AS (
           DELETE FROM batches
           WHERE topic_id = l_log.topic_id
               AND partition = l_log.partition
               AND last_offset < l_converted_offset
           RETURNING file_id, byte_size
        ),
        -- Remember what files were affected.
        _1 AS (
            INSERT INTO affected_files (file_id)
            SELECT DISTINCT file_id
            FROM deleted_batches
            ON CONFLICT DO NOTHING  -- ignore duplicates
        )
        SELECT COALESCE(SUM(byte_size), 0)
        FROM deleted_batches
        INTO l_deleted_bytes;

        UPDATE logs
        SET log_start_offset = l_converted_offset,
            byte_size = byte_size - l_deleted_bytes
        WHERE topic_id = l_log.topic_id
            AND partition = l_log.partition;

        RETURN NEXT (l_request.topic_id, l_request.partition, NULL, l_converted_offset)::delete_records_response_v1;
    END LOOP;

    -- Out of the affected files, select those that are now empty (i.e. no batch refers to them)
    -- and mark them for deletion.
    PERFORM mark_file_to_delete_v1(arg_now, file_id)
    FROM (
        SELECT DISTINCT af.file_id
        FROM affected_files AS af
            LEFT JOIN batches AS b ON af.file_id = b.file_id
        WHERE b.batch_id IS NULL
    );
END;
$$
;

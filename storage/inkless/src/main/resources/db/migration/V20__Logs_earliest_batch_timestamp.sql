-- Copyright (c) 2026 Aiven, Helsinki, Finland. https://aiven.io/

-- Adds logs.earliest_batch_timestamp: the effective timestamp (see batch_timestamp) of the batch
-- currently at log_start_offset, i.e. the oldest RETAINED batch of the partition. It lets a future
-- retention check decide "time retention deletes nothing" from this single column instead of
-- scanning the whole partition, for the steady-state case where the oldest batch is still within
-- retention. This migration only ADDS and MAINTAINS the column; no reader uses it yet.
--
-- NULL means "unknown, must scan": the value is populated lazily, so pre-existing rows and freshly
-- created logs start NULL and a future reader must fall back to a full scan until a commit or delete
-- populates it. This avoids a heavy backfill over the whole batches table at migration time.
--
-- Maintenance (both under the existing `logs FOR UPDATE`; the source of truth is the batches row,
-- never the request, so the value matches what a coalesced row (V14) stores):
--   * delete_records_v1: when a delete advances log_start the oldest retained batch changes, so
--     recompute from the new oldest batch (or NULL when the log becomes empty). No advance -> no
--     recompute.
--   * commit_file_v1 and commit_file_v2: when a partition in the commit has no value yet (NULL) populate
--     it from the oldest batch. This covers empty->non-empty (the just-committed batch is the oldest) and
--     lazily backfills pre-existing NULL logs (their existing oldest batch). Appends only add NEWER batches
--     at the head, so a log that already has a value is left untouched; steady-state commits skip the
--     recompute entirely. Both commit functions are live: commit_file_v1 is the default (batch coalescing
--     defaults off) and commit_file_v2 is used when coalescing is enabled, so both must maintain the column.
--   * prune_batches_below_highest_tiered_offset_v1: like delete_records_v1 it removes the oldest batches and
--     advances log_start (for cross-tier pruning), so it recomputes the value from the new oldest batch when
--     the prune actually deleted something (or NULLs it when the log becomes empty).

CREATE DOMAIN timestamp_nullable_t AS BIGINT
CHECK (VALUE IS NULL OR VALUE >= -5);

ALTER TABLE logs
ADD COLUMN earliest_batch_timestamp timestamp_nullable_t;

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
        ORDER BY topic_id, partition  -- ordering is important to prevent deadlocks
    LOOP
        SELECT *
        FROM logs
        WHERE topic_id = l_request.topic_id
            AND partition = l_request.partition
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
            byte_size = byte_size - l_deleted_bytes,
            -- Recompute only when the delete advanced log_start (the oldest retained batch changed).
            -- The subquery returns NULL when the log is now empty, which is the correct "unknown" state.
            earliest_batch_timestamp = CASE
                WHEN l_converted_offset > l_log.log_start_offset THEN (
                    SELECT batch_timestamp(b.timestamp_type, b.batch_max_timestamp, b.log_append_timestamp)
                    FROM batches b
                    WHERE b.topic_id = l_log.topic_id
                        AND b.partition = l_log.partition
                    ORDER BY b.topic_id, b.partition, b.last_offset
                    LIMIT 1
                )
                ELSE earliest_batch_timestamp
            END
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
        WHERE NOT EXISTS (
            SELECT 1
            FROM batches AS b
            WHERE b.file_id = af.file_id
        )
    );
END;
$$
;

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

    -- Open-run accumulator. A run is a maximal sequence of accepted, byte-contiguous
    -- requests sharing (topic_id, partition, magic, timestamp_type).
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
BEGIN
    l_log_append_timestamp := (EXTRACT(EPOCH FROM arg_now AT TIME ZONE 'UTC') * 1000)::BIGINT;

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

        -- This request is accepted. Either extend the open run, or flush it and open a new one.
        -- The byte-adjacency check (see header) guarantees no rejected batch intervened, so
        -- offsets within the run are contiguous.
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
        ELSE
            IF l_run_open THEN
                PERFORM flush_commit_run_v2(
                    l_new_file_id, l_run_magic, l_run_topic_id, l_run_partition,
                    l_run_base_offset, l_run_last_offset, l_run_byte_offset, l_run_byte_size,
                    l_run_timestamp_type, l_log_append_timestamp, l_run_max_timestamp);
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
        END IF;

        RETURN NEXT (l_request.topic_id, l_request.partition, l_log.log_start_offset, l_assigned_offset, l_request.batch_max_timestamp, 'none')::commit_batch_response_v1;
    END LOOP;

    -- Flush the final open run.
    IF l_run_open THEN
        PERFORM flush_commit_run_v2(
            l_new_file_id, l_run_magic, l_run_topic_id, l_run_partition,
            l_run_base_offset, l_run_last_offset, l_run_byte_offset, l_run_byte_size,
            l_run_timestamp_type, l_log_append_timestamp, l_run_max_timestamp);
    END IF;

    -- Transfer from the temporary to real table.
    UPDATE logs
    SET high_watermark = logs_tmp.high_watermark,
        byte_size = logs_tmp.byte_size
    FROM logs_tmp
    WHERE logs.topic_id = logs_tmp.topic_id
        AND logs.partition = logs_tmp.partition;

    -- Populate earliest_batch_timestamp for partitions in this commit that don't have it yet.
    -- IS NULL guards the hot path: appends never change the oldest batch, so a log that already
    -- has a value is skipped and the subquery never runs. This handles empty->non-empty (the
    -- just-committed batch is the oldest) and lazily backfills pre-existing NULL logs. A partition
    -- whose batches were all rejected stays empty, so the subquery yields NULL (still "unknown").
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
        AND l.earliest_batch_timestamp IS NULL;

    IF NOT EXISTS (SELECT 1 FROM batches WHERE file_id = l_new_file_id LIMIT 1) THEN
        PERFORM mark_file_to_delete_v1(arg_now, l_new_file_id);
    END IF;
END;
$$
;

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

    -- Populate earliest_batch_timestamp for partitions in this commit that don't have it yet (see header).
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
        AND l.earliest_batch_timestamp IS NULL;

    IF NOT EXISTS (SELECT 1 FROM batches WHERE file_id = l_new_file_id LIMIT 1) THEN
        PERFORM mark_file_to_delete_v1(arg_now, l_new_file_id);
    END IF;
END;
$$
;

CREATE OR REPLACE FUNCTION prune_batches_below_highest_tiered_offset_v1(
    arg_now TIMESTAMP WITH TIME ZONE,
    arg_requests prune_batches_below_highest_tiered_offset_request_v1[]
)
    RETURNS SETOF prune_batches_below_highest_tiered_offset_response_v1 LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    l_request prune_batches_below_highest_tiered_offset_request_v1;
    l_deleted_file_id BIGINT;
    l_deleted_file_ids BIGINT[];
    l_deleted_bytes BIGINT;
    l_new_log_start_offset BIGINT;
    l_log logs%ROWTYPE;
BEGIN
    IF arg_requests IS NOT NULL AND CARDINALITY(arg_requests) > 0 THEN
        PERFORM 1
        FROM logs l
        WHERE EXISTS(
            SELECT 1
            FROM unnest(arg_requests) AS r
            WHERE r.topic_id = l.topic_id AND r.partition = l.partition
        )
        ORDER BY l.topic_id, l.partition  -- ordering is important to prevent deadlocks
        FOR UPDATE;
        FOREACH l_request IN ARRAY arg_requests LOOP
            SELECT *
            FROM logs
            WHERE topic_id = l_request.topic_id
              AND partition = l_request.partition
            ORDER BY topic_id, partition
            FOR UPDATE
            INTO l_log;

            IF NOT FOUND THEN
                RETURN NEXT (
                    l_request.topic_id,
                    l_request.partition,
                    NULL,
                    'unknown_topic_or_partition'::prune_batches_below_highest_tiered_offset_error_v1
                )::prune_batches_below_highest_tiered_offset_response_v1;
                CONTINUE;
            END IF;

            WITH deleted AS (
                DELETE FROM batches
                    WHERE topic_id = l_request.topic_id
                        AND partition = l_request.partition
                        AND last_offset <= l_request.highest_tiered_offset
                    RETURNING file_id, byte_size
            )
            SELECT COALESCE(SUM(byte_size), 0), COALESCE(ARRAY_AGG(DISTINCT file_id), ARRAY[]::BIGINT[])
            FROM deleted
            INTO l_deleted_bytes, l_deleted_file_ids;

            FOREACH l_deleted_file_id IN ARRAY l_deleted_file_ids LOOP
                IF NOT EXISTS(SELECT 1 FROM batches WHERE file_id = l_deleted_file_id LIMIT 1) THEN
                    PERFORM mark_file_to_delete_v1(arg_now, l_deleted_file_id);
                END IF;
            END LOOP;

            SELECT MIN(base_offset)
            FROM batches
            WHERE topic_id = l_request.topic_id
              AND partition = l_request.partition
            INTO l_new_log_start_offset;

            IF l_new_log_start_offset IS NULL THEN
                l_new_log_start_offset := LEAST(
                    l_log.high_watermark,
                    GREATEST(l_request.highest_tiered_offset + 1, l_log.log_start_offset)
                );
            ELSE
                l_new_log_start_offset := GREATEST(l_log.log_start_offset, l_new_log_start_offset);
            END IF;

            UPDATE logs
            SET log_start_offset = l_new_log_start_offset,
                byte_size = byte_size - l_deleted_bytes,
                -- Recompute only when the prune actually removed batches (the oldest retained batch changed).
                -- The subquery returns NULL when the log is now empty, which is the correct "unknown" state.
                earliest_batch_timestamp = CASE
                    WHEN CARDINALITY(l_deleted_file_ids) > 0 THEN (
                        SELECT batch_timestamp(b.timestamp_type, b.batch_max_timestamp, b.log_append_timestamp)
                        FROM batches b
                        WHERE b.topic_id = l_request.topic_id
                            AND b.partition = l_request.partition
                        ORDER BY b.topic_id, b.partition, b.last_offset
                        LIMIT 1
                    )
                    ELSE earliest_batch_timestamp
                END
            WHERE topic_id = l_request.topic_id AND partition = l_request.partition;

            RETURN NEXT (
                l_request.topic_id,
                l_request.partition,
                l_new_log_start_offset,
                'none'::prune_batches_below_highest_tiered_offset_error_v1
            )::prune_batches_below_highest_tiered_offset_response_v1;
        END LOOP;
    END IF;
END;
$$
;

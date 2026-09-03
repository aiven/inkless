-- Copyright (c) 2026 Aiven, Helsinki, Finland. https://aiven.io/
-- Soft-delete diskless topics so DELETE_TOPICS no longer deletes every batch on the
-- controller event-handler thread (KC-349).
--
-- delete_topic_v1 only stamps logs.deleted_at. Live readers treat a stamped row as
-- gone (same as a missing log). purge_deleted_logs_v1 later drains batches with a
-- cap, marks newly empty files, and drops the log plus producer_state when the
-- partition has no batches left. FileCleaner still deletes the objects.

ALTER TABLE logs
ADD COLUMN deleted_at TIMESTAMP WITH TIME ZONE;

CREATE INDEX logs_by_deleted_at_idx
    ON logs (deleted_at)
    WHERE deleted_at IS NOT NULL;

-- The old delete_topic_v1 dropped logs without touching producer_state. Those
-- orphans have no log row, so the purger never sees them.
DELETE FROM producer_state ps
WHERE NOT EXISTS (
    SELECT 1
    FROM logs l
    WHERE l.topic_id = ps.topic_id
        AND l.partition = ps.partition
);

CREATE OR REPLACE FUNCTION delete_topic_v1(
    arg_now TIMESTAMP WITH TIME ZONE,
    arg_topic_ids UUID[]
)
RETURNS VOID LANGUAGE plpgsql VOLATILE AS $$
BEGIN
    -- Wait for in-flight commits that already hold the log row, then stamp.
    -- Later commits select only deleted_at IS NULL, so they see nonexistent_log.
    PERFORM
    FROM logs
    WHERE topic_id = ANY(arg_topic_ids)
        AND deleted_at IS NULL
    ORDER BY topic_id, partition
    FOR UPDATE;

    UPDATE logs
    SET deleted_at = arg_now
    WHERE topic_id = ANY(arg_topic_ids)
        AND deleted_at IS NULL;
END;
$$
;

CREATE TYPE purge_deleted_logs_response_v1 AS (
    batches_deleted BIGINT,
    logs_purged INT,
    files_marked INT,
    more_remain BOOLEAN
);

CREATE FUNCTION purge_deleted_logs_v1(
    arg_now TIMESTAMP WITH TIME ZONE,
    arg_max_batches INT
)
RETURNS SETOF purge_deleted_logs_response_v1 LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    l_batches_deleted BIGINT := 0;
    l_logs_purged INT := 0;
    l_files_marked INT := 0;
    l_more_remain BOOLEAN := FALSE;
    l_log RECORD;
    l_remaining BIGINT;
    l_deleted_this_log BIGINT;
    l_boundary offset_nullable_t;
BEGIN
    -- The partial index answers this probe. Without it, an idle cluster can walk
    -- logs_pkey filtering deleted_at on every live row, which is the produce hot path.
    IF NOT EXISTS (
        SELECT 1
        FROM logs
        WHERE deleted_at IS NOT NULL
    ) THEN
        RETURN NEXT (0, 0, 0, FALSE)::purge_deleted_logs_response_v1;
        RETURN;
    END IF;

    DROP TABLE IF EXISTS purge_affected_files;
    CREATE TEMPORARY TABLE purge_affected_files (
        file_id BIGINT PRIMARY KEY
    ) ON COMMIT DROP;

    -- Drop empty deleted logs without spending the batch budget. The same per-cycle
    -- cap still applies: a wide empty topic (many partitions, no batches) must not
    -- take an unbounded number of row locks in one transaction. Leftovers keep
    -- more_remain true for the next cycle. 0 / NULL means unbounded.
    -- deleted_at first so logs_by_deleted_at_idx drives the scan and older deletes go first.
    FOR l_log IN
        SELECT topic_id, partition
        FROM logs
        WHERE deleted_at IS NOT NULL
            AND NOT EXISTS (
                SELECT 1
                FROM batches b
                WHERE b.topic_id = logs.topic_id
                    AND b.partition = logs.partition
            )
        ORDER BY deleted_at, topic_id, partition
        LIMIT (CASE WHEN arg_max_batches > 0 THEN arg_max_batches END)
        FOR UPDATE SKIP LOCKED
    LOOP
        DELETE FROM producer_state
        WHERE topic_id = l_log.topic_id
            AND partition = l_log.partition;
        DELETE FROM logs
        WHERE topic_id = l_log.topic_id
            AND partition = l_log.partition;
        l_logs_purged := l_logs_purged + 1;
    END LOOP;

    l_remaining := CASE WHEN arg_max_batches > 0 THEN arg_max_batches::bigint ELSE NULL END;

    -- LIMIT is the batch budget: one batch per log is the worst case. Without it the planner
    -- can sort every soft-deleted log before the first DELETE.
    FOR l_log IN
        SELECT topic_id, partition
        FROM logs
        WHERE deleted_at IS NOT NULL
            AND EXISTS (
                SELECT 1
                FROM batches b
                WHERE b.topic_id = logs.topic_id
                    AND b.partition = logs.partition
            )
        ORDER BY deleted_at, topic_id, partition
        LIMIT (CASE WHEN arg_max_batches > 0 THEN arg_max_batches END)
        FOR UPDATE SKIP LOCKED
    LOOP
        EXIT WHEN l_remaining IS NOT NULL AND l_remaining <= 0;

        -- Same shape as enforce_retention_v2: probe the (cap+1)-th last_offset (index-only
        -- on the covering index), then range-delete. Selecting batch_id and deleting by
        -- PK heap-fetches every chosen row and descends batches_pkey again.
        l_boundary := NULL;
        IF l_remaining IS NOT NULL THEN
            SELECT last_offset
            FROM batches
            WHERE topic_id = l_log.topic_id
                AND partition = l_log.partition
            ORDER BY topic_id, partition, last_offset
            LIMIT 1 OFFSET l_remaining
            INTO l_boundary;
        END IF;

        WITH deleted_batches AS (
            DELETE FROM batches
            WHERE topic_id = l_log.topic_id
                AND partition = l_log.partition
                AND (l_boundary IS NULL OR last_offset < l_boundary)
            RETURNING file_id
        ),
        _1 AS (
            INSERT INTO purge_affected_files (file_id)
            SELECT DISTINCT file_id
            FROM deleted_batches
            ON CONFLICT DO NOTHING
        )
        SELECT COUNT(*)
        FROM deleted_batches
        INTO l_deleted_this_log;

        l_batches_deleted := l_batches_deleted + l_deleted_this_log;
        IF l_remaining IS NOT NULL THEN
            l_remaining := l_remaining - l_deleted_this_log;
        END IF;

        IF NOT EXISTS (
            SELECT 1
            FROM batches
            WHERE topic_id = l_log.topic_id
                AND partition = l_log.partition
            LIMIT 1
        ) THEN
            DELETE FROM producer_state
            WHERE topic_id = l_log.topic_id
                AND partition = l_log.partition;
            DELETE FROM logs
            WHERE topic_id = l_log.topic_id
                AND partition = l_log.partition;
            l_logs_purged := l_logs_purged + 1;
        END IF;
    END LOOP;

    -- Lock affected files in id order before the emptiness check. Two brokers can
    -- drain different partitions of the same WAL file (SKIP LOCKED on logs). Without
    -- this lock, neither transaction sees the other's uncommitted deletes, so both
    -- skip mark_file_to_delete_v1 and the file stays uploaded with zero batches.
    PERFORM 1
    FROM files
    WHERE file_id IN (SELECT file_id FROM purge_affected_files)
    ORDER BY file_id
    FOR UPDATE;

    SELECT COUNT(*)
    INTO l_files_marked
    FROM (
        SELECT DISTINCT af.file_id
        FROM purge_affected_files AS af
        WHERE NOT EXISTS (
            SELECT 1
            FROM batches AS b
            WHERE b.file_id = af.file_id
        )
    ) empty_files;

    PERFORM mark_file_to_delete_v1(arg_now, file_id)
    FROM (
        SELECT DISTINCT af.file_id
        FROM purge_affected_files AS af
        WHERE NOT EXISTS (
            SELECT 1
            FROM batches AS b
            WHERE b.file_id = af.file_id
        )
    ) empty_files;

    SELECT EXISTS (
        SELECT 1
        FROM logs
        WHERE deleted_at IS NOT NULL
    )
    INTO l_more_remain;

    RETURN NEXT (l_batches_deleted, l_logs_purged, l_files_marked, l_more_remain)::purge_deleted_logs_response_v1;
END;
$$
;

-- commit_file_v1 (from V20__Logs_earliest_batch_timestamp.sql): ignore soft-deleted logs.
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
            AND deleted_at IS NULL
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
        AND l.earliest_batch_timestamp IS NULL
        AND l.deleted_at IS NULL;

    IF NOT EXISTS (SELECT 1 FROM batches WHERE file_id = l_new_file_id LIMIT 1) THEN
        PERFORM mark_file_to_delete_v1(arg_now, l_new_file_id);
    END IF;
END;
$$
;

-- commit_file_v2 (from V20__Logs_earliest_batch_timestamp.sql): ignore soft-deleted logs.
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
            AND deleted_at IS NULL
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
        AND l.earliest_batch_timestamp IS NULL
        AND l.deleted_at IS NULL;

    IF NOT EXISTS (SELECT 1 FROM batches WHERE file_id = l_new_file_id LIMIT 1) THEN
        PERFORM mark_file_to_delete_v1(arg_now, l_new_file_id);
    END IF;
END;
$$
;

-- find_batches_v2 (from V27__Find_batches_request_level_min_one_message.sql): ignore soft-deleted logs.
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
        LEFT JOIN logs l ON r.topic_id = l.topic_id AND r.partition = l.partition AND l.deleted_at IS NULL
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

-- delete_records_v1 (from V20__Logs_earliest_batch_timestamp.sql): ignore soft-deleted logs.
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
            AND deleted_at IS NULL
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

-- enforce_retention_v2 (from V22__Retention_enforcement_bounded_scan.sql): ignore soft-deleted logs.
CREATE OR REPLACE FUNCTION enforce_retention_v2(
    arg_now TIMESTAMP WITH TIME ZONE,
    arg_requests enforce_retention_request_v1[],
    max_batches_per_request INT DEFAULT 0
)
RETURNS SETOF enforce_retention_response_v1 LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    l_request RECORD;
    l_log logs%ROWTYPE;
    l_base_offset_of_first_batch_to_keep offset_nullable_t;
    l_capped_offset offset_nullable_t;
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
            AND deleted_at IS NULL
        INTO l_log; -- NOTE: no FOR UPDATE

        IF NOT FOUND THEN
            RETURN NEXT (
                l_request.topic_id,
                l_request.partition,
                'unknown_topic_or_partition',
                NULL,
                NULL,
                NULL
            )::enforce_retention_response_v1;
            CONTINUE;
        END IF;

        l_base_offset_of_first_batch_to_keep := NULL;

        IF l_request.retention_bytes >= 0
            OR l_request.retention_ms >= 0
        THEN
            -- Short-circuit: if the oldest retained batch (the one at log_start) survives every enabled
            -- policy, nothing is deletable, so skip the boundary scan. The oldest batch is summarized
            -- on the log row: its reverse-aggregated size equals the whole log's byte_size, and its
            -- effective timestamp is logs.earliest_batch_timestamp (maintained by V20).
            -- earliest_batch_timestamp IS NULL means "unknown" (lazily populated), so we cannot prove
            -- anything and must scan.
            -- Reading these unlocked never over-deletes: the short-circuit only SKIPS work. For time the
            -- decision is stable (commits append newer batches at the head and deletes only make the oldest
            -- batch newer, so a proven "nothing to delete" stays true). For size a concurrent commit can grow
            -- byte_size past retention_bytes right after this read; that only defers the delete to the next
            -- enforcement cycle, exactly like the unlocked boundary scan below, which also decides on a snapshot.
            IF NOT (
                (l_request.retention_bytes < 0 OR l_log.byte_size <= l_request.retention_bytes)
                AND (l_request.retention_ms < 0
                    OR (l_log.earliest_batch_timestamp IS NOT NULL
                        AND l_log.earliest_batch_timestamp >= (EXTRACT(EPOCH FROM arg_now AT TIME ZONE 'UTC') * 1000)::BIGINT - l_request.retention_ms))
            ) THEN
                WITH selected_log AS (
                    SELECT byte_size
                    FROM logs
                    WHERE topic_id = l_request.topic_id
                        AND partition = l_request.partition
                        AND deleted_at IS NULL
                ),
                -- Scan only the oldest (max_batches_per_request + 1) batches. The retention boundary
                -- cannot be deeper than max_batches_per_request because the delete is capped there
                -- anyway, so scanning further just to compute a boundary we would immediately cap is
                -- the O(depth) cost being removed. The LIMIT is on the base-table scan, BEFORE the
                -- window, so it rides batches_by_last_offset_covering_idx index-only for O(cap) reads;
                -- placing it above the window would let the running SUM scan the whole partition first.
                -- LIMIT NULL (max_batches_per_request = 0) preserves the original unbounded full scan.
                limited_batches AS (
                    SELECT b.topic_id, b.partition, b.last_offset, b.base_offset, b.byte_size,
                        batch_timestamp(b.timestamp_type, b.batch_max_timestamp, b.log_append_timestamp) AS effective_timestamp
                    FROM batches b
                    WHERE b.topic_id = l_request.topic_id
                        AND b.partition = l_request.partition
                    ORDER BY b.topic_id, b.partition, b.last_offset
                    -- Cast to bigint before +1 so max_batches_per_request = INT_MAX does not overflow int.
                    LIMIT (CASE WHEN max_batches_per_request > 0 THEN max_batches_per_request::bigint + 1 ELSE NULL END)
                ),
                augmented_batches AS (
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
                    --     but doing so explicitly might be costly, hence the formula. It is exact over the window because
                    --     limited_batches holds the oldest batches in offset order, so the running SUM is the true
                    --     cumulative size of the first i batches and (SELECT byte_size FROM selected_log) is the true total.
                    -- For retention by time:
                    --     Associate with each batch its effective timestamp.
                    SELECT topic_id, partition, last_offset, base_offset,
                        (SELECT byte_size FROM selected_log)
                            - SUM(byte_size) OVER (ORDER BY topic_id, partition, last_offset)
                            + byte_size
                            AS reverse_agg_byte_size,
                        effective_timestamp
                    FROM limited_batches
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

                -- No batch in the scanned window complies. Two cases, both correct via high_watermark + cap-probe:
                --   (a) genuine delete-everything: fewer than cap+1 batches exist and none comply == delete up to HWM.
                --   (b) boundary is deeper than the cap: >= cap+1 batches are deletable. high_watermark would delete
                --       all, but the cap-probe below re-clamps to exactly max_batches_per_request, so this pass deletes
                --       cap and the next pass advances. The first cap batches are provably deletable (none of the first
                --       cap+1 complied), so this never over-deletes.
                l_base_offset_of_first_batch_to_keep := COALESCE(l_base_offset_of_first_batch_to_keep, l_log.high_watermark);
            END IF;
        END IF;

        -- Nothing to delete (retention disabled, or the oldest batch survives every enabled policy):
        -- report the log_start_offset from the unlocked read above and skip the lock entirely. This is
        -- the steady-state hot path; taking logs FOR UPDATE here would serialize a concurrent commit_file
        -- on the partition for no benefit, since we delete nothing. The reported offset is informational
        -- (trace logging in RetentionEnforcer) and a "nothing to delete" decision cannot be invalidated by
        -- a concurrent commit or delete. If the log was concurrently deleted this reports success with 0
        -- deleted rather than unknown_topic_or_partition, which is harmless (the delete proceeds anyway).
        IF l_base_offset_of_first_batch_to_keep IS NULL THEN
            RETURN NEXT (
                l_request.topic_id,
                l_request.partition,
                NULL,
                0,
                0::BIGINT,
                l_log.log_start_offset
            )::enforce_retention_response_v1;
            CONTINUE;
        END IF;

        -- take the lock only now, for the short delete. The boundary above was computed
        -- without the lock; delete_records_v1 re-clamps to the current log_start, so a concurrent
        -- commit (appends at the head) or delete (advances log_start at the tail) cannot cause
        -- over-deletion.
        SELECT *
        FROM logs
        WHERE topic_id = l_request.topic_id
            AND partition = l_request.partition
            AND deleted_at IS NULL
        INTO l_log
        FOR UPDATE;

        -- The log may have been deleted between the unlocked scan and here.
        IF NOT FOUND THEN
            RETURN NEXT (
                l_request.topic_id,
                l_request.partition,
                'unknown_topic_or_partition',
                NULL, NULL, NULL
            )::enforce_retention_response_v1;
            CONTINUE;
        END IF;

        -- Enforce max_batches_per_request against the CURRENT rows.
        -- Probe for the (max_batches_per_request + 1)-th deletable batch instead of counting all of them:
        -- OFFSET walks at most that many index entries, so the lock is held for O(max_batches_per_request),
        -- not O(deletable). Counting the whole deletable set here would reintroduce an O(depth) scan under
        -- the lock, defeating the point of computing the boundary unlocked.
        IF max_batches_per_request > 0 THEN
            SELECT base_offset
            FROM batches
            WHERE topic_id = l_request.topic_id
                AND partition = l_request.partition
                AND last_offset < l_base_offset_of_first_batch_to_keep
            ORDER BY topic_id, partition, last_offset
            LIMIT 1 OFFSET max_batches_per_request
            INTO l_capped_offset;

            -- If that batch exists, more than max_batches_per_request would be deleted: cap to its base
            -- offset so exactly max_batches_per_request are removed. Otherwise keep the full boundary.
            IF l_capped_offset IS NOT NULL THEN
                l_base_offset_of_first_batch_to_keep := l_capped_offset;
            END IF;
        END IF;

        -- Recount under the lock so the reported counts match exactly what delete_records_v1 removes.
        SELECT COUNT(*), SUM(byte_size)
        FROM batches
        WHERE topic_id = l_request.topic_id
            AND partition = l_request.partition
            AND last_offset < l_base_offset_of_first_batch_to_keep
        INTO l_batches_deleted, l_bytes_deleted;

        SELECT *
        FROM delete_records_v1(
            arg_now,
            array[ROW(
                l_request.topic_id,
                l_request.partition,
                l_base_offset_of_first_batch_to_keep
            )::delete_records_request_v1]
        )
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

-- list_offsets_v1 (from V17__List_offsets_cross_tier_earliest.sql): ignore soft-deleted logs.
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

-- init_diskless_log_v1 (from V23__Init_diskless_log_authoritative_seal.sql): ignore soft-deleted logs.
CREATE OR REPLACE FUNCTION init_diskless_log_v1(
    arg_requests init_diskless_log_request_v1[],
    arg_producer_states init_diskless_log_producer_state_v1[]
)
RETURNS SETOF init_diskless_log_response_v1 LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    l_request RECORD;
    l_producer_state RECORD;
BEGIN
    -- Every caller takes existing row locks in the same order; responses still follow arg_requests order.
    PERFORM 1
    FROM logs
    JOIN unnest(arg_requests) AS request
        ON logs.topic_id = request.topic_id
        AND logs.partition = request.partition
    ORDER BY logs.topic_id, logs.partition
    FOR UPDATE OF logs;

    FOR l_request IN
        SELECT *
        FROM unnest(arg_requests)
    LOOP
        IF l_request.diskless_start_offset < l_request.log_start_offset THEN
            RAISE EXCEPTION 'diskless_start_offset (%) must be >= log_start_offset (%) for topic_id=% partition=%',
                l_request.diskless_start_offset, l_request.log_start_offset,
                l_request.topic_id, l_request.partition;
        END IF;

        INSERT INTO logs (topic_id, partition, topic_name, log_start_offset, high_watermark, byte_size, diskless_start_offset)
        VALUES (l_request.topic_id, l_request.partition, l_request.topic_name,
                l_request.log_start_offset, l_request.diskless_start_offset, 0, l_request.diskless_start_offset)
        ON CONFLICT (topic_id, partition) DO UPDATE
            SET log_start_offset      = EXCLUDED.log_start_offset,
                high_watermark        = EXCLUDED.high_watermark,
                diskless_start_offset = EXCLUDED.diskless_start_offset
            WHERE logs.high_watermark = 0
                AND logs.diskless_start_offset = 0
                AND logs.byte_size = 0
                AND logs.deleted_at IS NULL
                -- A zero-offset re-init would not advance the placeholder, so leaving it alone keeps
                -- re-initing an existing empty partition a no-op.
                AND EXCLUDED.high_watermark > 0;

        IF NOT FOUND THEN
            RETURN NEXT (l_request.topic_id, l_request.partition, 'already_initialized')::init_diskless_log_response_v1;
            CONTINUE;
        END IF;

        -- The update path can reach a row an earlier init already wrote producer state for, and
        -- producer_state is keyed on a BIGSERIAL row_id, so those rows would accumulate rather than be
        -- replaced. The incoming snapshot is the authority for the partition.
        DELETE FROM producer_state
        WHERE topic_id = l_request.topic_id
            AND partition = l_request.partition;

        FOR l_producer_state IN
            SELECT *
            FROM unnest(arg_producer_states)
            WHERE topic_id = l_request.topic_id
                AND partition = l_request.partition
        LOOP
            INSERT INTO producer_state (
                topic_id, partition, producer_id,
                producer_epoch, base_sequence, last_sequence, assigned_offset, batch_max_timestamp
            )
            VALUES (
                l_producer_state.topic_id, l_producer_state.partition, l_producer_state.producer_id,
                l_producer_state.producer_epoch, l_producer_state.base_sequence, l_producer_state.last_sequence,
                l_producer_state.assigned_offset, l_producer_state.batch_max_timestamp
            );
        END LOOP;

        RETURN NEXT (l_request.topic_id, l_request.partition, 'none')::init_diskless_log_response_v1;
    END LOOP;
END;
$$
;

-- repair_diskless_log_v1 (from V18__Repair_diskless_log.sql): ignore soft-deleted logs.
CREATE OR REPLACE FUNCTION repair_diskless_log_v1(
    arg_requests repair_diskless_log_request_v1[]
)
RETURNS SETOF repair_diskless_log_response_v1 LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    l_request RECORD;
    l_found BOOLEAN;
BEGIN
    FOR l_request IN
        SELECT *
        FROM unnest(arg_requests)
        -- Order to avoid deadlocks with concurrent multi-row updates.
        ORDER BY topic_id, partition
    LOOP
        UPDATE logs
            SET diskless_start_offset = l_request.diskless_start_offset
        WHERE topic_id = l_request.topic_id
            AND partition = l_request.partition
            AND deleted_at IS NULL;

        l_found := FOUND;

        RETURN NEXT (l_request.topic_id, l_request.partition, l_found)::repair_diskless_log_response_v1;
    END LOOP;
END;
$$
;

-- advance_cross_tier_log_start_v1 (from V16__Cross_tier_log_start.sql): ignore soft-deleted logs.
CREATE OR REPLACE FUNCTION advance_cross_tier_log_start_v1(
    arg_requests advance_cross_tier_log_start_request_v1[]
)
RETURNS SETOF advance_cross_tier_log_start_response_v1 LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    l_request RECORD;
    l_log logs%ROWTYPE;
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
            AND deleted_at IS NULL
        INTO l_log
        FOR UPDATE;

        IF NOT FOUND THEN
            RETURN NEXT (l_request.topic_id, l_request.partition, 'unknown_topic_or_partition', NULL)::advance_cross_tier_log_start_response_v1;
            CONTINUE;
        END IF;

        IF l_log.remote_log_start_offset IS NULL OR l_request.remote_log_start_offset > l_log.remote_log_start_offset THEN
            UPDATE logs
            SET remote_log_start_offset = l_request.remote_log_start_offset
            WHERE topic_id = l_request.topic_id
                AND partition = l_request.partition;
            l_log.remote_log_start_offset = l_request.remote_log_start_offset;
        END IF;

        RETURN NEXT (
            l_request.topic_id,
            l_request.partition,
            NULL::advance_cross_tier_log_start_response_error_v1,
            l_log.remote_log_start_offset
        )::advance_cross_tier_log_start_response_v1;
    END LOOP;
END;
$$
;

-- prune_batches_below_highest_tiered_offset_v1 (from V20__Logs_earliest_batch_timestamp.sql): ignore soft-deleted logs.
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
          AND l.deleted_at IS NULL
        ORDER BY l.topic_id, l.partition  -- ordering is important to prevent deadlocks
        FOR UPDATE;
        FOREACH l_request IN ARRAY arg_requests LOOP
            SELECT *
            FROM logs
            WHERE topic_id = l_request.topic_id
              AND partition = l_request.partition
              AND deleted_at IS NULL
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


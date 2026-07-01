-- Copyright (c) 2026 Aiven, Helsinki, Finland. https://aiven.io/

-- commit_file_v2: identical semantics to commit_file_v1, except that contiguous
-- runs of accepted batches that belong to the same (topic_id, partition) and that
-- are byte-contiguous in the uploaded file are collapsed into a SINGLE `batches`
-- row. This drastically reduces `batches` table growth for low-throughput
-- producers that emit many small batches.
--
-- Invariants preserved versus v1:
--   * Per-request producer-state validation (epoch fencing, duplicate detection,
--     sequence ordering) is byte-for-byte the same. `producer_state` still gets
--     one row per physical batch, so idempotency / EOS is unchanged.
--   * The per-request response (assigned_base_offset, log_start_offset,
--     batch_timestamp, error) is identical to v1: one response per request, in
--     order, each carrying that physical batch's own assigned base offset.
--   * High-watermark progression and `logs.byte_size` accounting are identical.
--
-- The only observable difference is in the `batches` table: a coalesced row spans
-- multiple physical batches. Its byte range [byte_offset, byte_offset+byte_size)
-- contains those physical batches concatenated in order, so the read path
-- (FetchCompleter) can walk and relocate them. A run of length 1 produces a row
-- identical to v1.
--
-- Why byte-contiguity is the right (and sufficient) run condition:
--   The broker concatenates EVERY batch into the file, including ones the control
--   plane later rejects (duplicate / out-of-order / epoch). A rejected batch
--   consumes file bytes but does NOT advance the high watermark. Therefore, if two
--   accepted batches are byte-adjacent (next.byte_offset == prev.byte_offset +
--   prev.byte_size), no rejected batch can sit between them, so their offsets are
--   also contiguous. Byte-contiguity is strictly stronger than offset-contiguity,
--   so a single byte-adjacency check both (a) keeps a coalesced row's byte range
--   free of any rejected/dead bytes and (b) guarantees contiguous offsets. No
--   explicit "flush on reject" is needed: a reject breaks byte-adjacency, so the
--   next accepted batch naturally starts a fresh run.
--
-- DEPRECATION: commit_file_v1 is deprecated as of this migration. The broker always commits via
-- commit_file_v2; v1 is retained ONLY so that (a) an older broker binary mid-rolling-upgrade can still
-- commit (it calls v1) and (b) the coalescing benchmark can measure v2 against the v1 baseline. v1 must
-- not be dropped until no supported release calls it. Because v2 collapses multiple physical batches into
-- one row, the read path (FetchCompleter) must understand multi-batch rows BEFORE any broker writes them;
-- this migration is therefore safe to apply in a release whose read path already supports them, with the
-- write switch to v2 activated in that same or a later release.

-- Inserts a single `batches` row for a coalesced run. Extracted as a helper to
-- avoid repeating the INSERT at every run-break site.
CREATE OR REPLACE FUNCTION flush_commit_run_v2(
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
    arg_batch_max_timestamp timestamp_t
)
RETURNS VOID LANGUAGE plpgsql VOLATILE AS $$
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

    IF NOT EXISTS (SELECT 1 FROM batches WHERE file_id = l_new_file_id LIMIT 1) THEN
        PERFORM mark_file_to_delete_v1(arg_now, l_new_file_id);
    END IF;
END;
$$
;

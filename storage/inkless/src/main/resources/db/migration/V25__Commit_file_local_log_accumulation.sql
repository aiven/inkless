-- Copyright (c) 2026 Aiven, Helsinki, Finland. https://aiven.io/

-- commit_file_v2: identical semantics to the V20 definition. The only change is where the
-- per-request high-watermark/byte-size accumulation happens.
--
-- V20 (and every version back to V1) ran one `UPDATE logs_tmp ... RETURNING high_watermark` per
-- accepted request, purely to keep the `l_log` variable in sync with the staging table. `logs_tmp` is
-- created by CTAS and therefore has no index, so each of those updates seqscans one row per distinct
-- partition in the file: the staging cost of a commit was O(requests x partitions).
--
-- Measured on CommitFileFanOutCostBenchmarkTest (local container, 1600 batches per commit, constant
-- volume, only fan-out varying): commit time rose from ~104 ms at 16 partitions to ~208 ms at 1600,
-- tracking `requests x partitions` at ~40-49 ns per tuple visit. At a production shape of 630
-- partitions x 1600 batches that term alone is ~45 ms per commit, on top of the per-statement
-- execution overhead of the 1600 updates themselves.
--
-- Here the accumulation happens in `l_log` and is written back to `logs_tmp` once per partition run
-- (at each partition change, and once after the loop), so the staging cost becomes O(partitions).
--
-- Why this is equivalent:
--   * `l_log` was already the value the loop reads: `l_assigned_offset := l_log.high_watermark` and
--     the run bookkeeping both consume the variable, not the table. The old `RETURNING ... INTO`
--     existed only to refresh it.
--   * The write-back sets absolute values, not deltas, so re-reading a partition after a flush yields
--     the accumulated state. Correctness therefore does not depend on `arg_requests` being grouped by
--     partition (BatchBuffer.close() groups them, which keeps the flush count at one per partition,
--     but ungrouped input would only cost extra flushes).
--   * Every rejecting branch still CONTINUEs before the accumulation, exactly as it did before the
--     UPDATE, so a rejected request leaves the watermark untouched.
--   * A partition missing from `logs` leaves `l_log` all-NULL (SELECT INTO with no row); the flush is
--     guarded on `l_log.topic_id IS NOT NULL` so that state is never written back.
--
-- WAL is unchanged: `logs_tmp` is a temporary relation and is not WAL-logged, and the transfer into
-- the real `logs` table is still a single UPDATE per partition.
--
-- commit_file_v1 is deliberately left as it was: it is deprecated (retained only for mid-rolling-upgrade
-- brokers and as the coalescing benchmark's baseline), so it keeps the old accumulation.

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

            -- Publish what was accumulated for the partition we are leaving before losing the variable.
            IF l_log.topic_id IS NOT NULL THEN
                UPDATE logs_tmp
                SET high_watermark = l_log.high_watermark,
                    byte_size = l_log.byte_size
                WHERE topic_id = l_log.topic_id
                    AND partition = l_log.partition;
            END IF;

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

        -- Accumulate in the variable; the write-back happens once per partition run (see header).
        l_new_high_watermark := l_log.high_watermark + (l_request.last_offset - l_request.base_offset + 1);
        l_log.high_watermark := l_new_high_watermark;
        l_log.byte_size := l_log.byte_size + l_request.byte_size;

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

    -- Publish the last partition's accumulation, which no partition change flushed.
    IF l_log.topic_id IS NOT NULL THEN
        UPDATE logs_tmp
        SET high_watermark = l_log.high_watermark,
            byte_size = l_log.byte_size
        WHERE topic_id = l_log.topic_id
            AND partition = l_log.partition;
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

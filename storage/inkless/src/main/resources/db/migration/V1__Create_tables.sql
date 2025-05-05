-- Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
CREATE DOMAIN broker_id_t AS INT NOT NULL;

CREATE DOMAIN topic_id_t AS UUID NOT NULL;

CREATE DOMAIN partition_t AS INT NOT NULL
CHECK (VALUE >= 0);

CREATE DOMAIN topic_name_t VARCHAR(255) NOT NULL;

CREATE DOMAIN magic_t AS SMALLINT NOT NULL
CHECK (VALUE >= 0 AND VALUE <= 2);

CREATE DOMAIN format_t AS SMALLINT NOT NULL
CHECK (value >= 1 AND VALUE <= 3);

CREATE DOMAIN offset_nullable_t BIGINT
CHECK (VALUE IS NULL OR VALUE >= 0);
CREATE DOMAIN offset_t AS offset_nullable_t
CHECK (VALUE IS NOT NULL);
CREATE DOMAIN offset_with_minus_one_t BIGINT
CHECK (VALUE IS NOT NULL AND VALUE >= -1);

CREATE DOMAIN byte_offset_t BIGINT NOT NULL
CHECK (VALUE >= 0);

CREATE DOMAIN byte_size_t BIGINT NOT NULL  -- TODO replace with INT?
CHECK (VALUE >= 0);

CREATE DOMAIN object_key_t AS VARCHAR(1024) NOT NULL;

CREATE DOMAIN timestamp_type_t AS SMALLINT NOT NULL
CHECK (VALUE >= -1 AND VALUE <= 1);

CREATE DOMAIN timestamp_t AS BIGINT NOT NULL
CHECK (VALUE >= -5);

CREATE DOMAIN producer_id_t AS BIGINT NOT NULL
CHECK (VALUE >= -1);

CREATE DOMAIN producer_epoch_t AS SMALLINT NOT NULL
CHECK (VALUE >= -1);

CREATE DOMAIN sequence_t AS INT NOT NULL
CHECK (VALUE >= -1);

CREATE TABLE logs (
    topic_id topic_id_t,
    partition partition_t,
    topic_name topic_name_t,
    log_start_offset offset_t,
    high_watermark offset_t,
    PRIMARY KEY(topic_id, partition)
);

-- The reasons why a file on the remote storage exists.
CREATE TYPE file_reason_t AS ENUM (
    -- Uploaded by a broker as the result of producing.
    'produce',
    -- Uploaded by a broker as the result of merging.
    'merge'
);

CREATE TYPE file_state_t AS ENUM (
    -- Uploaded by a broker, in use, etc.
    'uploaded',
    -- Marked for deletion.
    'deleting'
);

CREATE TABLE files (
    file_id BIGSERIAL PRIMARY KEY,
    object_key object_key_t UNIQUE NOT NULL,
    format format_t,
    reason file_reason_t NOT NULL,
    state file_state_t NOT NULL,
    uploader_broker_id broker_id_t,
    committed_at TIMESTAMP WITH TIME ZONE,
    marked_for_deletion_at TIMESTAMP WITH TIME ZONE,
    size byte_size_t
);

CREATE INDEX files_by_state_only_deleting_idx ON files (state) WHERE state = 'deleting';

CREATE TABLE batches (
    batch_id BIGSERIAL PRIMARY KEY,
    magic magic_t,
    topic_id topic_id_t,
    partition partition_t,
    base_offset offset_t,
    last_offset offset_t,
    file_id BIGINT NOT NULL,
    byte_offset byte_offset_t,
    byte_size byte_size_t,
    timestamp_type timestamp_type_t,
    log_append_timestamp timestamp_t,
    batch_max_timestamp timestamp_t,
    CONSTRAINT fk_batches_logs FOREIGN KEY (topic_id, partition) REFERENCES logs(topic_id, partition)
        ON DELETE NO ACTION ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED,  -- allow deleting logs before batches
    CONSTRAINT fk_batches_files FOREIGN KEY (file_id) REFERENCES files(file_id) ON DELETE RESTRICT ON UPDATE CASCADE
);
CREATE INDEX batches_by_file ON batches (file_id);
CREATE INDEX batches_by_last_offset_idx ON batches (topic_id, partition, last_offset);

CREATE TABLE producer_state (
    topic_id topic_id_t,
    partition partition_t,
    producer_id producer_id_t,
    row_id BIGSERIAL,
    producer_epoch producer_epoch_t,
    base_sequence sequence_t,
    last_sequence sequence_t,
    assigned_offset offset_t,
    batch_max_timestamp timestamp_t,
    PRIMARY KEY (topic_id, partition, producer_id, row_id)
);

CREATE TYPE commit_batch_request_v1 AS (
    magic magic_t,
    topic_id topic_id_t,
    partition partition_t,
    byte_offset byte_offset_t,
    byte_size byte_size_t,
    base_offset offset_t,
    last_offset offset_t,
    timestamp_type timestamp_type_t,
    batch_max_timestamp timestamp_t,
    producer_id producer_id_t,
    producer_epoch producer_epoch_t,
    base_sequence sequence_t,
    last_sequence sequence_t
);

CREATE TYPE commit_batch_response_v1_error AS ENUM (
    'none',
    -- errors
    'nonexistent_log',
    'invalid_producer_epoch',
    'sequence_out_of_order',
    'duplicate_batch'
);

CREATE TYPE commit_batch_response_v1 AS (
    topic_id topic_id_t,
    partition partition_t,
    log_start_offset offset_nullable_t,
    assigned_base_offset offset_nullable_t,
    batch_timestamp timestamp_t,
    error commit_batch_response_v1_error
);

CREATE FUNCTION commit_file_v1(
    object_key object_key_t,
    format format_t,
    uploader_broker_id broker_id_t,
    file_size byte_size_t,
    now TIMESTAMP WITH TIME ZONE,
    requests commit_batch_request_v1[]
)
RETURNS SETOF commit_batch_response_v1 LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    new_file_id BIGINT;
    request RECORD;
    log logs%ROWTYPE;
    duplicate RECORD;
    assigned_offset offset_nullable_t;
    new_high_watermark offset_nullable_t;
    last_sequence_in_producer_epoch BIGINT;
BEGIN
    INSERT INTO files (object_key, format, reason, state, uploader_broker_id, committed_at, size)
    VALUES (object_key, format, 'produce', 'uploaded', uploader_broker_id, now, file_size)
    RETURNING file_id
    INTO new_file_id;

    -- We use this temporary table to perform the write operations in loop on it first
    -- and only then dump the result on the real table. This reduces the WAL pressure and latency of the function.
    CREATE TEMPORARY TABLE logs_tmp
    ON COMMIT DROP
    AS
        -- Extract the relevant logs into the temporary table and simultaneously lock them.
        -- topic_name and log_start_offset aren't technically needed, but having them allows declaring `log logs%ROWTYPE`.
        SELECT *
        FROM logs
        WHERE (topic_id, partition) IN (SELECT DISTINCT topic_id, partition FROM unnest(requests))
        ORDER BY topic_id, partition  -- ordering is important to prevent deadlocks
        FOR UPDATE;

    FOR request IN
        SELECT *
        FROM unnest(requests)
    LOOP
        -- A small optimization: select the log into a variable only if it's a different topic-partition.
        -- Batches are sorted by topic-partitions, so this makes sense.
        IF log.topic_id IS DISTINCT FROM request.topic_id
            OR log.partition IS DISTINCT FROM request.partition THEN

            SELECT *
            FROM logs_tmp
            WHERE topic_id = request.topic_id
                AND partition = request.partition
            INTO log;

            IF NOT FOUND THEN
                RETURN NEXT (request.topic_id, request.partition, NULL, NULL, -1, 'nonexistent_log')::commit_batch_response_v1;
                CONTINUE;
            END IF;

        END IF;

        assigned_offset = log.high_watermark;

        -- Validate that the new request base sequence is not larger than the previous batch last sequence
        IF request.producer_id > -1 AND request.producer_epoch > -1
        THEN
            -- If there are previous batches for the producer, check that the producer epoch is not smaller than the last batch
             IF EXISTS (
                SELECT 1
                FROM producer_state
                WHERE topic_id = request.topic_id
                    AND partition = request.partition
                    AND producer_id = request.producer_id
                    AND producer_epoch > request.producer_epoch
             ) THEN
                RETURN NEXT (request.topic_id, request.partition, NULL, NULL, -1, 'invalid_producer_epoch')::commit_batch_response_v1;
                CONTINUE;
             END IF;

             SELECT MAX(last_sequence)
             INTO last_sequence_in_producer_epoch
             FROM producer_state
             WHERE topic_id = request.topic_id
                 AND partition = request.partition
                 AND producer_id = request.producer_id
                 AND producer_epoch = request.producer_epoch;

            -- If there are previous batches for the producer
            IF last_sequence_in_producer_epoch IS NULL THEN
                -- If there are no previous batches for the producer, the base sequence must be 0
                IF request.base_sequence <> 0
                THEN
                    RETURN NEXT (request.topic_id, request.partition, NULL, NULL, -1, 'sequence_out_of_order')::commit_batch_response_v1;
                    CONTINUE;
                END IF;
            ELSE
                -- Check for duplicates
                SELECT *
                FROM producer_state
                WHERE topic_id = request.topic_id
                    AND partition = request.partition
                    AND producer_id = request.producer_id
                    AND producer_epoch = request.producer_epoch
                    AND base_sequence = request.base_sequence
                    AND last_sequence = request.last_sequence
                INTO duplicate;
                IF FOUND THEN
                    RETURN NEXT (request.topic_id, request.partition, log.log_start_offset, duplicate.assigned_offset, duplicate.batch_max_timestamp, 'duplicate_batch')::commit_batch_response_v1;
                    CONTINUE;
                END IF;

                -- Check that the sequence is not out of order.
                -- A sequence is out of order if the base sequence is not a continuation of the last sequence
                -- or, in case of wraparound, the base sequence must be 0 and the last sequence must be 2147483647 (Integer.MAX_VALUE).
                IF (request.base_sequence - 1) <> last_sequence_in_producer_epoch OR (last_sequence_in_producer_epoch = 2147483647 AND request.base_sequence <> 0) THEN
                    RETURN NEXT (request.topic_id, request.partition, NULL, NULL, -1, 'sequence_out_of_order')::commit_batch_response_v1;
                    CONTINUE;
                END IF;
            END IF;

            INSERT INTO producer_state (
                topic_id, partition, producer_id,
                producer_epoch, base_sequence, last_sequence, assigned_offset, batch_max_timestamp
            )
            VALUES (
                request.topic_id, request.partition, request.producer_id,
                request.producer_epoch, request.base_sequence, request.last_sequence, assigned_offset, request.batch_max_timestamp
            );
            -- Keep only the last 5 records.
            -- 5 == org.apache.kafka.storage.internals.log.ProducerStateEntry.NUM_BATCHES_TO_RETAIN
            DELETE FROM producer_state
            WHERE topic_id = request.topic_id
                AND partition = request.partition
                AND producer_id = request.producer_id
                AND row_id <= (
                    SELECT row_id
                    FROM producer_state
                    WHERE topic_id = request.topic_id
                        AND partition = request.partition
                        AND producer_id = request.producer_id
                    ORDER BY row_id DESC
                    LIMIT 1
                    OFFSET 5
                );
        END IF;

        UPDATE logs_tmp
        SET high_watermark = high_watermark + (request.last_offset - request.base_offset + 1)
        WHERE topic_id = request.topic_id
            AND partition = request.partition
        RETURNING high_watermark
        INTO new_high_watermark;

        log.high_watermark = new_high_watermark;

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
            request.magic,
            request.topic_id, request.partition,
            assigned_offset,
            new_high_watermark - 1,
            new_file_id,
            request.byte_offset, request.byte_size,
            request.timestamp_type,
            (EXTRACT(EPOCH FROM now AT TIME ZONE 'UTC') * 1000)::BIGINT,
            request.batch_max_timestamp
        );

        RETURN NEXT (request.topic_id, request.partition, log.log_start_offset, assigned_offset, request.batch_max_timestamp, 'none')::commit_batch_response_v1;
    END LOOP;

    -- Transfer from the temporary to real table.
    UPDATE logs
    SET high_watermark = logs_tmp.high_watermark
    FROM logs_tmp
    WHERE logs.topic_id = logs_tmp.topic_id
        AND logs.partition = logs_tmp.partition;

    IF NOT EXISTS (SELECT 1 FROM batches WHERE file_id = new_file_id LIMIT 1) THEN
        PERFORM mark_file_to_delete_v1(now, new_file_id);
    END IF;
END;
$$
;

CREATE FUNCTION delete_topic_v1(
    now TIMESTAMP WITH TIME ZONE,
    arg_topic_ids UUID[]
)
RETURNS VOID LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    log RECORD;
BEGIN
    FOR log IN
        DELETE FROM logs
        WHERE topic_id = ANY(arg_topic_ids)
        RETURNING logs.*
    LOOP
        PERFORM delete_batch_v1(now, batch_id)
        FROM batches
        WHERE topic_id = log.topic_id
            AND partition = log.partition;
    END LOOP;
END;
$$
;

CREATE TYPE delete_records_request_v1 AS (
    topic_id topic_id_t,
    partition partition_t,
    "offset" BIGINT
);

CREATE TYPE delete_records_response_v1_error_t AS ENUM (
    'unknown_topic_or_partition', 'offset_out_of_range'
);

CREATE TYPE delete_records_response_v1 AS (
    topic_id topic_id_t,
    partition partition_t,
    error delete_records_response_v1_error_t,
    log_start_offset offset_nullable_t
);

CREATE FUNCTION delete_records_v1(
    now TIMESTAMP WITH TIME ZONE,
    requests delete_records_request_v1[]
)
RETURNS SETOF delete_records_response_v1 LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    request RECORD;
    log RECORD;
    converted_offset BIGINT = -1;
BEGIN
    FOR request IN
        SELECT *
        FROM unnest(requests)
    LOOP
        SELECT *
        FROM logs
        WHERE topic_id = request.topic_id
            AND partition = request.partition
        FOR UPDATE
        INTO log;

        IF NOT FOUND THEN
            RETURN NEXT (request.topic_id, request.partition, 'unknown_topic_or_partition', NULL)::delete_records_response_v1;
            CONTINUE;
        END IF;

        converted_offset = CASE
            -- -1 = org.apache.kafka.common.requests.DeleteRecordsRequest.HIGH_WATERMARK
            WHEN request.offset = -1 THEN log.high_watermark
            ELSE request.offset
        END;

        IF converted_offset < 0 OR converted_offset > log.high_watermark THEN
            RETURN NEXT (request.topic_id, request.partition, 'offset_out_of_range', NULL)::delete_records_response_v1;
            CONTINUE;
        END IF;

        IF converted_offset > log.log_start_offset THEN
            UPDATE logs
            SET log_start_offset = converted_offset
            WHERE topic_id = log.topic_id
                AND partition = log.partition;
            log.log_start_offset = converted_offset;
        END IF;

        PERFORM delete_batch_v1(now, batches.batch_id)
        FROM batches
        WHERE topic_id = log.topic_id
            AND partition = log.partition
            AND last_offset < log.log_start_offset;

        RETURN NEXT (request.topic_id, request.partition, NULL, log.log_start_offset)::delete_records_response_v1;
    END LOOP;
END;
$$
;

CREATE FUNCTION delete_batch_v1(
    now TIMESTAMP WITH TIME ZONE,
    arg_batch_id BIGINT
)
RETURNS VOID LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    l_file_id BIGINT;
BEGIN
    DELETE FROM batches
    WHERE batch_id = arg_batch_id
    RETURNING file_id
    INTO l_file_id;

    IF NOT EXISTS (SELECT 1 FROM batches WHERE file_id = l_file_id LIMIT 1) THEN
        PERFORM mark_file_to_delete_v1(now, l_file_id);
    END IF;
END;
$$
;

CREATE FUNCTION mark_file_to_delete_v1(
    now TIMESTAMP WITH TIME ZONE,
    arg_file_id BIGINT
)
RETURNS VOID LANGUAGE plpgsql VOLATILE AS $$
BEGIN
    UPDATE files
    SET state = 'deleting',
        marked_for_deletion_at = now
    WHERE file_id = arg_file_id;
END;
$$
;

CREATE FUNCTION delete_files_v1(
    paths object_key_t[]
)
RETURNS VOID LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    file RECORD;
BEGIN
    FOR file IN
        SELECT *
        FROM files
        WHERE object_key = ANY(paths)
            AND state = 'deleting'
        FOR UPDATE
    LOOP
        DELETE FROM file_merge_work_item_files
        WHERE file_id = file.file_id;

        DELETE FROM files
        WHERE file_id = file.file_id;
    END LOOP;
END;
$$
;

CREATE TYPE list_offsets_request_v1 AS (
    topic_id topic_id_t,
    partition partition_t,
    timestamp timestamp_t
);

CREATE TYPE list_offsets_response_error_v1 AS ENUM (
    'none',
    -- errors
    'unknown_topic_or_partition',
    'unsupported_special_timestamp'
);

CREATE TYPE list_offsets_response_v1 AS (
    topic_id topic_id_t,
    partition partition_t,
    timestamp timestamp_t,
    "offset" offset_with_minus_one_t,
    error list_offsets_response_error_v1
);

CREATE FUNCTION list_offsets_v1(
    requests list_offsets_request_v1[]
)
RETURNS SETOF list_offsets_response_v1 LANGUAGE plpgsql STABLE AS $$
DECLARE
    request RECORD;
    log RECORD;
    max_timestamp BIGINT = NULL;
    found_timestamp BIGINT = NULL;
    found_timestamp_offset BIGINT = NULL;
BEGIN
    FOR request IN
        SELECT *
        FROM unnest(requests)
    LOOP
        -- Note that we're not doing locking ("FOR UPDATE") here, as it's not really needed for this read-only function.
        SELECT *
        FROM logs
        WHERE topic_id = request.topic_id
            AND partition = request.partition
        INTO log;

        IF NOT FOUND THEN
            -- -1 = org.apache.kafka.common.record.RecordBatch.NO_TIMESTAMP
            RETURN NEXT (request.topic_id, request.partition, -1, -1, 'unknown_topic_or_partition')::list_offsets_response_v1;
            CONTINUE;
        END IF;

        -- -2 = org.apache.kafka.common.requests.ListOffsetsRequest.EARLIEST_TIMESTAMP
        -- -4 = org.apache.kafka.common.requests.ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP
        IF request.timestamp = -2 OR request.timestamp = -4 THEN
            -- -1 = org.apache.kafka.common.record.RecordBatch.NO_TIMESTAMP
            RETURN NEXT (request.topic_id, request.partition, -1, log.log_start_offset, 'none')::list_offsets_response_v1;
            CONTINUE;
        END IF;

        -- -1 = org.apache.kafka.common.requests.ListOffsetsRequest.LATEST_TIMESTAMP
        IF request.timestamp = -1 THEN
            -- -1 = org.apache.kafka.common.record.RecordBatch.NO_TIMESTAMP
            RETURN NEXT (request.topic_id, request.partition, -1, log.high_watermark, 'none')::list_offsets_response_v1;
            CONTINUE;
        END IF;

        -- -3 = org.apache.kafka.common.requests.ListOffsetsRequest.MAX_TIMESTAMP
        IF request.timestamp = -3 THEN
            SELECT MAX(batch_timestamp(timestamp_type, batch_max_timestamp, log_append_timestamp))
            INTO max_timestamp
            FROM batches
            WHERE topic_id = request.topic_id
                AND partition = request.partition;

            SELECT last_offset
            INTO found_timestamp_offset
            FROM batches
            WHERE topic_id = request.topic_id
                AND partition = request.partition
                AND batch_timestamp(timestamp_type, batch_max_timestamp, log_append_timestamp) = max_timestamp
            ORDER BY batch_id
            LIMIT 1;

            IF found_timestamp_offset IS NULL THEN
                -- -1 = org.apache.kafka.common.record.RecordBatch.NO_TIMESTAMP
                RETURN NEXT (request.topic_id, request.partition, -1, -1, 'none')::list_offsets_response_v1;
            ELSE
                RETURN NEXT (request.topic_id, request.partition, max_timestamp, found_timestamp_offset, 'none')::list_offsets_response_v1;
            END IF;
            CONTINUE;
        END IF;

        -- -5 = org.apache.kafka.common.requests.ListOffsetsRequest.LATEST_TIERED_TIMESTAMP
        IF request.timestamp = -5 THEN
            -- -1 = org.apache.kafka.common.record.RecordBatch.NO_TIMESTAMP
            RETURN NEXT (request.topic_id, request.partition, -1, -1, 'none')::list_offsets_response_v1;
            CONTINUE;
        END IF;

        IF request.timestamp < 0 THEN
            -- Unsupported special timestamp.
            -- -1 = org.apache.kafka.common.record.RecordBatch.NO_TIMESTAMP
            RETURN NEXT (request.topic_id, request.partition, -1, -1, 'unsupported_special_timestamp')::list_offsets_response_v1;
            CONTINUE;
        END IF;

        SELECT batch_timestamp(timestamp_type, batch_max_timestamp, log_append_timestamp), base_offset
        INTO found_timestamp, found_timestamp_offset
        FROM batches
        WHERE topic_id = request.topic_id
            AND partition = request.partition
            AND batch_timestamp(timestamp_type, batch_max_timestamp, log_append_timestamp) >= request.timestamp
        ORDER BY batch_id
        LIMIT 1;

        IF found_timestamp_offset IS NULL THEN
            -- -1 = org.apache.kafka.common.record.RecordBatch.NO_TIMESTAMP
            RETURN NEXT (request.topic_id, request.partition, -1, -1, 'none')::list_offsets_response_v1;
        ELSE
            RETURN NEXT (
                request.topic_id, request.partition, found_timestamp,
                GREATEST(found_timestamp_offset, log.log_start_offset),
                'none'
            )::list_offsets_response_v1;
        END IF;
        CONTINUE;
    END LOOP;
END;
$$
;

CREATE TABLE file_merge_work_items (
    work_item_id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE file_merge_work_item_files (
    work_item_id BIGINT REFERENCES file_merge_work_items(work_item_id),
    file_id BIGINT REFERENCES files(file_id),
    PRIMARY KEY (work_item_id, file_id)
);

CREATE TYPE batch_metadata_v1 AS (
    magic magic_t,
    topic_id topic_id_t,
    topic_name topic_name_t,
    partition partition_t,

    byte_offset byte_offset_t,
    byte_size byte_size_t,
    base_offset offset_t,
    last_offset offset_t,
    log_append_timestamp timestamp_t,
    batch_max_timestamp timestamp_t,
    timestamp_type timestamp_type_t
);

CREATE TYPE file_merge_work_item_response_v1_batch AS (
    batch_id BIGINT,
    object_key object_key_t,
    metadata batch_metadata_v1
);

CREATE TYPE file_merge_work_item_response_v1_file AS (
    file_id BIGINT,
    object_key object_key_t,
    format format_t,
    size byte_size_t,
    batches file_merge_work_item_response_v1_batch[]
);

CREATE TYPE file_merge_work_item_response_v1 AS (
    work_item_id BIGINT,
    created_at TIMESTAMP WITH TIME ZONE,
    file_ids file_merge_work_item_response_v1_file[]
);

CREATE FUNCTION get_file_merge_work_item_v1(
    now TIMESTAMP WITH TIME ZONE,
    expiration_interval INTERVAL,
    merge_file_size_threshold byte_size_t
)
RETURNS SETOF file_merge_work_item_response_v1 LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    expired_work_item RECORD;
    file_ids BIGINT[];
    new_work_item_id BIGINT;
    existing_file_id BIGINT;
BEGIN
    -- Delete any expired work items
    FOR expired_work_item IN
        SELECT *
        FROM file_merge_work_items
        WHERE created_at <= now - expiration_interval
    LOOP
        DELETE FROM file_merge_work_item_files
        WHERE work_item_id = expired_work_item.work_item_id;

        DELETE FROM file_merge_work_items
        WHERE work_item_id = expired_work_item.work_item_id;
    END LOOP;

    -- Identify files to merge based on threshold size
    WITH file_candidates AS (
    SELECT
        file_id,
        committed_at,
        size
    FROM files
    WHERE state = 'uploaded'
        AND reason != 'merge'
        AND NOT EXISTS (
            SELECT 1
            FROM file_merge_work_item_files
            WHERE file_id = files.file_id
        )
    ),
    running_sums AS (
        SELECT
            file_id,
            size,
            SUM(size) OVER (
                ORDER BY committed_at, file_id
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) as cumulative_size,
            SUM(size) OVER (
                ORDER BY committed_at, file_id
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ) as previous_sum
        FROM file_candidates
    ),
    threshold_point AS (
        SELECT MIN(file_id) as last_file_id
        FROM running_sums
        WHERE cumulative_size >= merge_file_size_threshold
    )
    SELECT array_agg(rs.file_id ORDER BY rs.file_id)
    INTO file_ids
    FROM running_sums rs
    WHERE rs.file_id <= (SELECT last_file_id FROM threshold_point);

    -- Return if no files to merge
    IF file_ids IS NULL OR array_length(file_ids, 1) = 0 THEN
        RETURN;
    END IF;

    -- Create new work item
    INSERT INTO file_merge_work_items(created_at)
    VALUES (now)
    RETURNING work_item_id
    INTO new_work_item_id;

    -- Add files to work item
    FOREACH existing_file_id IN ARRAY file_ids
    LOOP
        INSERT INTO file_merge_work_item_files(work_item_id, file_id)
        VALUES (new_work_item_id, existing_file_id);
    END LOOP;

    -- Return work item
    RETURN NEXT (
        new_work_item_id,
        now,
        ARRAY(
            SELECT (
                f.file_id,
                files.object_key,
                files.format,
                files.size,
                ARRAY(
                    SELECT (
                        batches.batch_id,
                        files.object_key,
                        (
                            batches.magic,
                            logs.topic_id,
                            logs.topic_name,
                            batches.partition,
                            batches.byte_offset,
                            batches.byte_size,
                            batches.base_offset,
                            batches.last_offset,
                            batches.log_append_timestamp,
                            batches.batch_max_timestamp,
                            batches.timestamp_type
                        )::batch_metadata_v1
                    )::file_merge_work_item_response_v1_batch
                    FROM batches
                        JOIN files ON batches.file_id = files.file_id
                        JOIN logs ON batches.topic_id = logs.topic_id AND batches.partition = logs.partition
                    WHERE batches.file_id = f.file_id
                )
            )::file_merge_work_item_response_v1_file
            FROM unnest(file_ids) AS f(file_id)
            JOIN files ON f.file_id = files.file_id
        )
    )::file_merge_work_item_response_v1;
END;
$$
;

CREATE TYPE commit_file_merge_work_item_v1_batch AS (
    metadata batch_metadata_v1,
    parent_batch_ids BIGINT[]
);

CREATE TYPE commit_file_merge_work_item_v1_error AS ENUM (
    'none',
    'file_merge_work_item_not_found',
    'invalid_parent_batch_count',
    'batch_not_part_of_work_item'
);


CREATE TYPE commit_file_merge_work_item_v1_response AS (
    error commit_file_merge_work_item_v1_error,
    error_batch commit_file_merge_work_item_v1_batch
);

CREATE FUNCTION commit_file_merge_work_item_v1(
    now TIMESTAMP WITH TIME ZONE,
    existing_work_item_id BIGINT,
    object_key object_key_t,
    format format_t,
    uploader_broker_id broker_id_t,
    file_size byte_size_t,
    merge_file_batches commit_file_merge_work_item_v1_batch[]
)
RETURNS commit_file_merge_work_item_v1_response LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    work_item RECORD;
    new_file_id BIGINT;
    found_batches_size BIGINT;
    work_item_file RECORD;
    merge_file_batch commit_file_merge_work_item_v1_batch;
BEGIN
    -- check that the work item exists
    SELECT * FROM file_merge_work_items
    WHERE work_item_id = existing_work_item_id
    FOR UPDATE
    INTO work_item;

    IF NOT FOUND THEN
        -- do not remove the file if this condition is hit because it may be a retry from a valid work item
        -- only delete the object key when a failure condition is found

        RETURN ROW('file_merge_work_item_not_found'::commit_file_merge_work_item_v1_error, NULL)::commit_file_merge_work_item_v1_response;
    END IF;

    -- check that the number of parent batches is 1 (limitation of the current implementation)
    FOR merge_file_batch IN
        SELECT *
        FROM unnest(merge_file_batches) b
    LOOP
        IF array_length(merge_file_batch.parent_batch_ids, 1) IS NULL OR array_length(merge_file_batch.parent_batch_ids, 1) != 1 THEN
            -- insert new empty file to be deleted
            INSERT INTO files (object_key, format, reason, state, uploader_broker_id, committed_at, size)
            VALUES (object_key, format, 'merge', 'uploaded', uploader_broker_id, now, 0)
            RETURNING file_id
            INTO new_file_id;
            PERFORM mark_file_to_delete_v1(now, new_file_id);

            RETURN ROW('invalid_parent_batch_count'::commit_file_merge_work_item_v1_error, merge_file_batch)::commit_file_merge_work_item_v1_response;
        END IF;
    END LOOP;

    -- filter merge_file_batches to only include the ones where logs exist
    merge_file_batches := ARRAY(
        SELECT b
        FROM unnest(merge_file_batches) b
        JOIN batches ON b.parent_batch_ids[1] = batches.batch_id
        JOIN logs ON batches.topic_id = logs.topic_id AND batches.partition = logs.partition
    );

    -- check if the found batch file id is part of the work item
    SELECT SUM(batches.byte_size)
    FROM batches
    WHERE EXISTS (
        SELECT 1
        FROM unnest(merge_file_batches) b
        WHERE batch_id = ANY(b.parent_batch_ids)
    )
    INTO found_batches_size;

    IF found_batches_size IS NULL THEN
        -- insert new empty file
        INSERT INTO files (object_key, format, reason, state, uploader_broker_id, committed_at, size)
        VALUES (object_key, format, 'merge', 'uploaded', uploader_broker_id, now, 0)
        RETURNING file_id
        INTO new_file_id;
        PERFORM mark_file_to_delete_v1(now, new_file_id);

        -- delete work item
        PERFORM release_file_merge_work_item_v1(existing_work_item_id);

        RETURN ROW('none'::commit_file_merge_work_item_v1_error, NULL)::commit_file_merge_work_item_v1_response;
    END IF;

    -- check that all parent batch files are part of work item files
    FOR merge_file_batch IN
        SELECT *
        FROM unnest(merge_file_batches) b
        WHERE NOT EXISTS (
            SELECT 1
            FROM file_merge_work_item_files
                JOIN batches ON file_merge_work_item_files.file_id = batches.file_id
            WHERE work_item_id = existing_work_item_id
                AND batch_id = ANY(b.parent_batch_ids)
        )
    LOOP
        -- insert new empty file to be deleted
        INSERT INTO files (object_key, format, reason, state, uploader_broker_id, committed_at, size)
        VALUES (object_key, format, 'merge', 'uploaded', uploader_broker_id, now, 0)
        RETURNING file_id
        INTO new_file_id;
        PERFORM mark_file_to_delete_v1(now, new_file_id);

        RETURN ROW('batch_not_part_of_work_item'::commit_file_merge_work_item_v1_error, merge_file_batch)::commit_file_merge_work_item_v1_response;
    END LOOP;

    -- delete old files
    FOR work_item_file IN
        SELECT file_id
        FROM file_merge_work_item_files AS f
        WHERE work_item_id = existing_work_item_id
    LOOP
        PERFORM mark_file_to_delete_v1(now, work_item_file.file_id);
    END LOOP;

    -- insert new file
    INSERT INTO files (object_key, format, reason, state, uploader_broker_id, committed_at, size)
    VALUES (object_key, format, 'merge', 'uploaded', uploader_broker_id, now, file_size)
    RETURNING file_id
    INTO new_file_id;

    -- delete old batches
    DELETE FROM batches
    WHERE EXISTS (
        SELECT 1
        FROM unnest(merge_file_batches) b
        WHERE batch_id = ANY(b.parent_batch_ids)
    );

    -- insert new batches
    INSERT INTO batches (
        magic,
        topic_id, partition,
        base_offset,
        last_offset,
        file_id,
        byte_offset, byte_size,
        log_append_timestamp,
        batch_max_timestamp,
        timestamp_type
    )
    SELECT DISTINCT
        (unnest(merge_file_batches)).metadata.magic,
        (unnest(merge_file_batches)).metadata.topic_id,
        (unnest(merge_file_batches)).metadata.partition,
        (unnest(merge_file_batches)).metadata.base_offset,
        (unnest(merge_file_batches)).metadata.last_offset,
        new_file_id,
        (unnest(merge_file_batches)).metadata.byte_offset,
        (unnest(merge_file_batches)).metadata.byte_size,
        (unnest(merge_file_batches)).metadata.log_append_timestamp,
        (unnest(merge_file_batches)).metadata.batch_max_timestamp,
        (unnest(merge_file_batches)).metadata.timestamp_type
    FROM unnest(merge_file_batches)
    ORDER BY (unnest(merge_file_batches)).metadata.topic_id,
        (unnest(merge_file_batches)).metadata.partition,
        (unnest(merge_file_batches)).metadata.base_offset;

    -- delete work item
    PERFORM release_file_merge_work_item_v1(existing_work_item_id);

    RETURN ROW('none'::commit_file_merge_work_item_v1_error, NULL)::commit_file_merge_work_item_v1_response;
END;
$$
;

CREATE TYPE release_file_merge_work_item_v1_error AS ENUM (
    'none',
    'file_merge_work_item_not_found'
);

CREATE TYPE release_file_merge_work_item_v1_response AS (
    error release_file_merge_work_item_v1_error
);

CREATE FUNCTION release_file_merge_work_item_v1(
    existing_work_item_id BIGINT
)
RETURNS release_file_merge_work_item_v1_response LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    work_item RECORD;
BEGIN
    SELECT * FROM file_merge_work_items
    WHERE work_item_id = existing_work_item_id
    FOR UPDATE
    INTO work_item;

    IF NOT FOUND THEN
        RETURN ROW('file_merge_work_item_not_found'::release_file_merge_work_item_v1_error)::release_file_merge_work_item_v1_response;
    END IF;

    DELETE FROM file_merge_work_item_files
    WHERE work_item_id = existing_work_item_id;

    DELETE FROM file_merge_work_items
    WHERE work_item_id = existing_work_item_id;

    RETURN ROW('none'::release_file_merge_work_item_v1_error)::release_file_merge_work_item_v1_response;
END;
$$
;

CREATE FUNCTION batch_timestamp(
    timestamp_type timestamp_type_t,
    batch_max_timestamp timestamp_t,
    log_append_timestamp timestamp_t
)
RETURNS timestamp_t LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    -- See how timestamps are assigned in
    -- https://github.com/aiven/inkless/blob/e124d3975bdb3a9ec85eee2fba7a1b0a6967d3a6/storage/src/main/java/org/apache/kafka/storage/internals/log/LogValidator.java#L271-L276
    RETURN CASE timestamp_type
       WHEN 1 THEN log_append_timestamp  -- org.apache.kafka.common.record.TimestampType.LOG_APPEND_TIME
       ELSE batch_max_timestamp
   END;
END
$$
;

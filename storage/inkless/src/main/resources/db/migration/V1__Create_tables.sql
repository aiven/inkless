-- Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
CREATE DOMAIN broker_id_t AS INT NOT NULL;

CREATE DOMAIN topic_id_t AS UUID NOT NULL;

CREATE DOMAIN partition_t AS INT NOT NULL
CHECK (VALUE >= 0);

CREATE DOMAIN topic_name_t VARCHAR(255) NOT NULL;

CREATE DOMAIN offset_nullable_t BIGINT
CHECK (VALUE IS NULL OR VALUE >= 0);
CREATE DOMAIN offset_t AS offset_nullable_t
CHECK (VALUE IS NOT NULL);

CREATE DOMAIN byte_offset_t BIGINT NOT NULL
CHECK (VALUE >= 0);

CREATE DOMAIN byte_size_t BIGINT NOT NULL  -- TODO replace with INT?
CHECK (VALUE >= 0);

CREATE DOMAIN object_key_t AS VARCHAR(1024) NOT NULL;

CREATE DOMAIN timestamp_type_t AS SMALLINT NOT NULL
CHECK (VALUE >= -1 AND VALUE <= 1);

CREATE DOMAIN timestamp_t AS BIGINT NOT NULL
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
    'produce'
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
    reason file_reason_t NOT NULL,
    state file_state_t NOT NULL,
    uploader_broker_id broker_id_t,
    committed_at TIMESTAMP WITH TIME ZONE,
    size byte_size_t,
    used_size byte_size_t
);

CREATE TABLE files_to_delete (
    file_id BIGINT PRIMARY KEY,
    marked_for_deletion_at TIMESTAMP WITH TIME ZONE,
    CONSTRAINT fk_files_to_delete_files FOREIGN KEY (file_id) REFERENCES files(file_id) ON DELETE RESTRICT ON UPDATE CASCADE
);

CREATE TABLE batches (
    batch_id BIGSERIAL PRIMARY KEY,
    topic_id topic_id_t,
    partition partition_t,
    base_offset offset_t,
    last_offset offset_t,
    request_base_offset offset_t,
    request_last_offset offset_t,
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

CREATE INDEX batches_by_last_offset_idx ON batches (topic_id, partition, last_offset);

CREATE TYPE commit_batch_request_v1 AS (
    topic_id topic_id_t,
    partition partition_t,
    byte_offset byte_offset_t,
    byte_size byte_size_t,
    request_base_offset offset_t,
    request_last_offset offset_t,
    timestamp_type timestamp_type_t,
    batch_max_timestamp timestamp_t
);

CREATE TYPE commit_batch_response_v1 AS (
    topic_id topic_id_t,
    partition partition_t,
    log_exists BOOLEAN,
    assigned_offset offset_nullable_t,
    log_start_offset offset_nullable_t
);

CREATE FUNCTION commit_file_v1(
    object_key object_key_t,
    uploader_broker_id broker_id_t,
    file_size byte_size_t,
    now TIMESTAMP WITH TIME ZONE,
    requests commit_batch_request_v1[]
)
RETURNS SETOF commit_batch_response_v1 LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    new_file_id BIGINT;
    request RECORD;
    log RECORD;
    assigned_offset offset_nullable_t;
    new_high_watermark offset_nullable_t;
BEGIN
    INSERT INTO files (object_key, reason, state, uploader_broker_id, committed_at, size, used_size)
    VALUES (object_key, 'produce', 'uploaded', uploader_broker_id, now, file_size, file_size)
    RETURNING file_id
    INTO new_file_id;

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
            RETURN NEXT (request.topic_id, request.partition, FALSE, NULL, NULL)::commit_batch_response_v1;
            CONTINUE;
        END IF;

        assigned_offset = log.high_watermark;

        UPDATE logs
        SET high_watermark = high_watermark + (request.request_last_offset - request.request_base_offset + 1)
        WHERE topic_id = request.topic_id
            AND partition = request.partition
        RETURNING high_watermark
        INTO new_high_watermark;

        INSERT INTO batches (
            topic_id, partition,
            base_offset,
            last_offset,
            request_base_offset,
            request_last_offset,
            file_id,
            byte_offset, byte_size,
            timestamp_type,
            log_append_timestamp,
            batch_max_timestamp
        )
        VALUES (
            request.topic_id, request.partition,
            assigned_offset,
            new_high_watermark - 1,
            request.request_base_offset, request.request_last_offset,
            new_file_id,
            request.byte_offset, request.byte_size,
            request.timestamp_type,
            (EXTRACT(EPOCH FROM now AT TIME ZONE 'UTC') * 1000)::BIGINT,
            request.batch_max_timestamp
        );

        RETURN NEXT (request.topic_id, request.partition, TRUE, assigned_offset, log.log_start_offset)::commit_batch_response_v1;
    END LOOP;
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
        PERFORM delete_batch_v1(now, topic_id, partition, base_offset)
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

        PERFORM delete_batch_v1(now, batches.topic_id, batches.partition, batches.base_offset)
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
    arg_topic_id topic_id_t,
    arg_partition partition_t,
    arg_base_offset offset_t
)
RETURNS VOID LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    l_file_id BIGINT;
    batch_size byte_size_t = 0;
    new_used_size byte_size_t = 0;
BEGIN
    DELETE FROM batches
    WHERE topic_id = arg_topic_id
        AND partition = arg_partition
        AND base_offset = arg_base_offset
    RETURNING file_id, byte_size
    INTO l_file_id, batch_size;

    UPDATE files
    SET used_size = used_size - batch_size
    WHERE file_id = l_file_id
    RETURNING used_size
    INTO new_used_size;

    IF new_used_size = 0 THEN
        PERFORM delete_file_v1(now, l_file_id);
    END IF;
END;
$$
;

CREATE FUNCTION delete_file_v1(
    now TIMESTAMP WITH TIME ZONE,
    arg_file_id BIGINT
)
RETURNS VOID LANGUAGE plpgsql VOLATILE AS $$
BEGIN
    UPDATE files
    SET state = 'deleting'
    WHERE file_id = arg_file_id;

    INSERT INTO files_to_delete(file_id, marked_for_deletion_at)
    VALUES (arg_file_id, now);
END;
$$
;

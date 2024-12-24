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
    producer_id producer_id_t,
    producer_epoch producer_epoch_t,
    base_sequence sequence_t,
    last_sequence sequence_t,
    PRIMARY KEY(topic_id, partition, base_offset),
    CONSTRAINT fk_batches_logs FOREIGN KEY (topic_id, partition) REFERENCES logs(topic_id, partition)
        ON DELETE NO ACTION ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED,  -- allow deleting logs before batches
    CONSTRAINT fk_batches_files FOREIGN KEY (file_id) REFERENCES files(file_id) ON DELETE RESTRICT ON UPDATE CASCADE
);

CREATE INDEX batches_by_last_offset_idx ON batches (topic_id, partition, last_offset);

CREATE INDEX batches_by_producer_idx ON batches (producer_id, producer_epoch);

CREATE TYPE commit_batch_response_v1 AS (
    topic_id topic_id_t,
    partition partition_t,
    log_exists BOOLEAN,
    assigned_offset offset_nullable_t,
    log_start_offset offset_nullable_t,
    invalid_producer_epoch BOOLEAN,
    sequence_out_of_order BOOLEAN
);

CREATE FUNCTION commit_file_v1(
    object_key object_key_t,
    uploader_broker_id broker_id_t,
    file_size byte_size_t,
    now TIMESTAMP WITH TIME ZONE,
    requests JSONB
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
        FROM jsonb_to_recordset(requests)
            r(
                topic_id topic_id_t,
                partition partition_t,
                byte_offset byte_offset_t,
                byte_size byte_size_t,
                request_base_offset offset_t,
                request_last_offset offset_t,
                timestamp_type timestamp_type_t,
                batch_max_timestamp timestamp_t,
                producer_id producer_id_t,
                producer_epoch producer_epoch_t,
                base_sequence sequence_t,
                last_sequence sequence_t
            )
    LOOP
        SELECT *
        FROM logs
        WHERE topic_id = request.topic_id
            AND partition = request.partition
        FOR UPDATE
        INTO log;

        IF NOT FOUND THEN
            RETURN NEXT (request.topic_id, request.partition, FALSE, NULL, NULL, FALSE, FALSE)::commit_batch_response_v1;
            CONTINUE;
        END IF;

        -- Validate that the new request base sequence is not larger than the previous batch last sequence
        IF request.producer_id > -1 AND request.producer_epoch > -1
        THEN
             IF EXISTS (
                SELECT 1
                FROM batches
                WHERE topic_id = request.topic_id
                    AND partition = request.partition
                    AND producer_id = request.producer_id
                    AND producer_epoch > request.producer_epoch
             ) THEN
                RETURN NEXT (request.topic_id, request.partition, TRUE, NULL, NULL, TRUE, FALSE)::commit_batch_response_v1;
                CONTINUE;
             END IF;
            -- If there are previous batches for the producer, check that the base sequence is not larger than the last sequence
            IF EXISTS (
                SELECT 1
                FROM batches
                WHERE topic_id = request.topic_id
                  AND partition = request.partition
                  AND producer_id = request.producer_id
                  AND producer_epoch = request.producer_epoch
            ) THEN
                IF EXISTS (
                    SELECT 1
                    FROM batches
                    WHERE topic_id = request.topic_id
                        AND partition = request.partition
                        AND producer_id = request.producer_id
                        AND producer_epoch = request.producer_epoch
                        AND last_sequence = (
                            SELECT MAX(last_sequence)
                            FROM batches
                            WHERE topic_id = request.topic_id
                            AND partition = request.partition
                            AND producer_id = request.producer_id
                            AND producer_epoch = request.producer_epoch
                        )
                        -- sequence is out of order if the base sequence is not a continuation of the last sequence
                        AND request.base_sequence - 1 != last_sequence
                        -- if end of int32 range, the base sequence must be 0 and the last sequence must be 2147483647
                        AND NOT (request.base_sequence = 0 AND last_sequence = 2147483647)
                ) THEN
                    RETURN NEXT (request.topic_id, request.partition, TRUE, NULL, NULL, FALSE, TRUE)::commit_batch_response_v1;
                    CONTINUE;
                END IF;
            ELSE
                IF request.base_sequence != 0
                THEN
                    RETURN NEXT (request.topic_id, request.partition, TRUE, NULL, NULL, FALSE, TRUE)::commit_batch_response_v1;
                    CONTINUE;
                END IF;
            END IF;
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
            timestamp_type, log_append_timestamp, batch_max_timestamp,
            producer_id, producer_epoch, base_sequence, last_sequence
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
            request.batch_max_timestamp,
            request.producer_id, request.producer_epoch, request.base_sequence, request.last_sequence
        );

        RETURN NEXT (request.topic_id, request.partition, TRUE, assigned_offset, log.log_start_offset, FALSE, FALSE)::commit_batch_response_v1;
    END LOOP;
END;
$$
;

CREATE FUNCTION delete_topic_v1(
    now TIMESTAMP WITH TIME ZONE,
    arg_topic_ids JSONB
)
RETURNS VOID LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    log RECORD;
BEGIN
    FOR log IN
        WITH topic_ids AS
            (SELECT value::uuid AS topic_id FROM jsonb_array_elements_text(arg_topic_ids))
        DELETE FROM logs
        WHERE topic_id IN (SELECT topic_id FROM topic_ids)
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

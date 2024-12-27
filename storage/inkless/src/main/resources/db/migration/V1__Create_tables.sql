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

CREATE DOMAIN number_of_records_t AS BIGINT NOT NULL  -- TODO replace with INT?
CHECK (VALUE >= 0);

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

CREATE TABLE files (
    file_id BIGSERIAL PRIMARY KEY,
    object_key object_key_t UNIQUE NOT NULL,
    reason file_reason_t NOT NULL,
    uploader_broker_id broker_id_t,
    committed_at TIMESTAMP WITH TIME ZONE,
    size byte_size_t,
    used_size byte_size_t
);

CREATE TABLE batches (
    topic_id topic_id_t,
    partition partition_t,
    base_offset offset_t,
    last_offset offset_t,
    file_id BIGINT NOT NULL,
    byte_offset byte_offset_t,
    byte_size byte_size_t,
    number_of_records number_of_records_t,
    timestamp_type timestamp_type_t,
    log_append_timestamp timestamp_t,
    batch_max_timestamp timestamp_t,
    PRIMARY KEY(topic_id, partition, base_offset),
    CONSTRAINT fk_batches_files FOREIGN KEY (file_id) REFERENCES files(file_id) ON DELETE RESTRICT ON UPDATE CASCADE
);

CREATE INDEX batches_by_last_offset_idx ON batches (topic_id, partition, last_offset);

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
    INSERT INTO files (object_key, reason, uploader_broker_id, committed_at, size, used_size)
    VALUES (object_key, 'produce', uploader_broker_id, now, file_size, file_size)
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
                number_of_records number_of_records_t,
                timestamp_type timestamp_type_t,
                batch_max_timestamp timestamp_t
            )
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
        SET high_watermark = high_watermark + request.number_of_records
        WHERE topic_id = request.topic_id
            AND partition = request.partition
        RETURNING high_watermark
        INTO new_high_watermark;

        INSERT INTO batches (
            topic_id, partition,
            base_offset,
            last_offset,
            file_id,
            byte_offset, byte_size, number_of_records,
            timestamp_type,
            log_append_timestamp,
            batch_max_timestamp
        )
        VALUES (
            request.topic_id, request.partition,
            assigned_offset,
            new_high_watermark - 1,
            new_file_id,
            request.byte_offset, request.byte_size, request.number_of_records,
            request.timestamp_type,
            (EXTRACT(EPOCH FROM now AT TIME ZONE 'UTC') * 1000)::BIGINT,
            request.batch_max_timestamp
        );

        RETURN NEXT (request.topic_id, request.partition, TRUE, assigned_offset, log.log_start_offset)::commit_batch_response_v1;
    END LOOP;
END;
$$
;

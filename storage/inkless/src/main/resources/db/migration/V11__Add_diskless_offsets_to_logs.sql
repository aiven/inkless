-- Copyright (c) 2026 Aiven, Helsinki, Finland. https://aiven.io/
CREATE DOMAIN leader_epoch_t AS INT NOT NULL
CHECK (VALUE >= 0);

ALTER TABLE logs ADD COLUMN diskless_start_offset offset_t DEFAULT 0;
ALTER TABLE logs ADD COLUMN diskless_end_offset offset_nullable_t DEFAULT NULL;
ALTER TABLE logs ADD COLUMN leader_epoch_at_init leader_epoch_t DEFAULT 0;

CREATE TYPE init_diskless_log_producer_state_v1 AS (
    producer_id producer_id_t,
    producer_epoch producer_epoch_t,
    base_sequence sequence_t,
    last_sequence sequence_t,
    assigned_offset offset_t,
    batch_max_timestamp timestamp_t
);

CREATE TYPE init_diskless_log_request_v1 AS (
    topic_id topic_id_t,
    partition partition_t,
    topic_name topic_name_t,
    log_start_offset offset_t,
    diskless_start_offset offset_t,
    leader_epoch leader_epoch_t,
    producer_state init_diskless_log_producer_state_v1[]
);

CREATE TYPE init_diskless_log_response_error_v1 AS ENUM (
    'none',
    'already_initialized'
);

CREATE TYPE init_diskless_log_response_v1 AS (
    topic_id topic_id_t,
    partition partition_t,
    error init_diskless_log_response_error_v1
);

-- Init diskless log function:
-- - Rejects with already_initialized if log already exists
CREATE FUNCTION init_diskless_log_v1(
    arg_requests init_diskless_log_request_v1[]
)
RETURNS SETOF init_diskless_log_response_v1 LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    l_request RECORD;
    l_existing_log RECORD;
    l_producer_state RECORD;
BEGIN
    FOR l_request IN
        SELECT *
        FROM unnest(arg_requests)
    LOOP
        -- Check if log already exists
        SELECT topic_id, partition, leader_epoch_at_init, diskless_start_offset, high_watermark
        INTO l_existing_log
        FROM logs
        WHERE topic_id = l_request.topic_id
          AND partition = l_request.partition;

        IF FOUND THEN
            -- Log already exists - reject initialization
            RETURN NEXT (l_request.topic_id, l_request.partition, 'already_initialized')::init_diskless_log_response_v1;
            CONTINUE;
        ELSE
            -- Insert new log record
            INSERT INTO logs (
                topic_id,
                partition,
                topic_name,
                log_start_offset,
                high_watermark,
                byte_size,
                diskless_start_offset,
                leader_epoch_at_init
            )
            VALUES (
                l_request.topic_id,
                l_request.partition,
                l_request.topic_name,
                l_request.log_start_offset,
                l_request.diskless_start_offset,
                0,
                l_request.diskless_start_offset,
                l_request.leader_epoch
            );
        END IF;

        -- Insert producer state entries
        IF l_request.producer_state IS NOT NULL THEN
            FOR l_producer_state IN
                SELECT *
                FROM unnest(l_request.producer_state)
            LOOP
                INSERT INTO producer_state (
                    topic_id,
                    partition,
                    producer_id,
                    producer_epoch,
                    base_sequence,
                    last_sequence,
                    assigned_offset,
                    batch_max_timestamp
                )
                VALUES (
                    l_request.topic_id,
                    l_request.partition,
                    l_producer_state.producer_id,
                    l_producer_state.producer_epoch,
                    l_producer_state.base_sequence,
                    l_producer_state.last_sequence,
                    l_producer_state.assigned_offset,
                    l_producer_state.batch_max_timestamp
                );
            END LOOP;
        END IF;

        RETURN NEXT (l_request.topic_id, l_request.partition, 'none')::init_diskless_log_response_v1;
    END LOOP;
END;
$$
;
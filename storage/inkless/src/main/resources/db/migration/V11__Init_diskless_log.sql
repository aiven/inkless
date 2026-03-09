-- Copyright (c) 2026 Aiven, Helsinki, Finland. https://aiven.io/

ALTER TABLE logs ADD COLUMN diskless_start_offset offset_t DEFAULT 0;

CREATE TYPE init_diskless_log_error_v1 AS ENUM (
    'none',
    'already_initialized'
);

CREATE TYPE init_diskless_log_request_v1 AS (
    topic_id topic_id_t,
    partition partition_t,
    topic_name topic_name_t,
    log_start_offset offset_t,
    diskless_start_offset offset_t
);

CREATE TYPE init_diskless_log_producer_state_v1 AS (
    topic_id topic_id_t,
    partition partition_t,
    producer_id producer_id_t,
    producer_epoch producer_epoch_t,
    base_sequence sequence_t,
    last_sequence sequence_t,
    assigned_offset offset_t,
    batch_max_timestamp timestamp_t
);

CREATE TYPE init_diskless_log_response_v1 AS (
    topic_id topic_id_t,
    partition partition_t,
    error init_diskless_log_error_v1
);

CREATE OR REPLACE FUNCTION init_diskless_log_v1(
    arg_requests init_diskless_log_request_v1[],
    arg_producer_states init_diskless_log_producer_state_v1[]
)
RETURNS SETOF init_diskless_log_response_v1 LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    l_request RECORD;
    l_producer_state RECORD;
    l_inserted BOOLEAN;
BEGIN
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
        ON CONFLICT (topic_id, partition) DO NOTHING;

        l_inserted := FOUND;

        IF NOT l_inserted THEN
            RETURN NEXT (l_request.topic_id, l_request.partition, 'already_initialized')::init_diskless_log_response_v1;
            CONTINUE;
        END IF;

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

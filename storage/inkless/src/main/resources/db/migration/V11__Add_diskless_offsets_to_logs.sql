-- Copyright (c) 2026 Aiven, Helsinki, Finland. https://aiven.io/

-- Add columns for diskless migration tracking
CREATE DOMAIN leader_epoch_t AS INT NOT NULL
CHECK (VALUE >= 0);

ALTER TABLE logs ADD COLUMN diskless_start_offset offset_t DEFAULT 0;
ALTER TABLE logs ADD COLUMN diskless_end_offset offset_nullable_t DEFAULT NULL;
ALTER TABLE logs ADD COLUMN leader_epoch_at_init leader_epoch_t DEFAULT 0;

-- Types for init diskless log function
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

-- Response error type for init diskless log function
CREATE TYPE init_diskless_log_response_error_v1 AS ENUM (
    'none',
    'invalid_diskless_start_offset'  -- Protocol violation: different disklessStartOffset was passed for same partition
);

CREATE TYPE init_diskless_log_response_v1 AS (
    topic_id topic_id_t,
    partition partition_t,
    error init_diskless_log_response_error_v1
);

-- Init diskless log function with idempotent semantics and B0 mismatch detection.
--
-- Due to the Delos-style chain sealing protocol:
-- 1. Metadata records are processed in order (config change before leader change)
-- 2. Sealing happens inside the write lock in makeLeader (no produce window)
-- 3. All leaders converge to the same B0 (LEO at seal time)
--
-- Therefore, all init requests for the same partition SHOULD have identical B0.
-- If B0 differs, it indicates a protocol violation (bug) that must be investigated.
--
-- Returns:
-- - 'none': Success (inserted or idempotent match)
-- - 'invalid_diskless_start_offset': Error - partition exists with different disklessStartOffset (protocol violation)
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
        SELECT topic_id, partition, diskless_start_offset
        INTO l_existing_log
        FROM logs
        WHERE topic_id = l_request.topic_id
          AND partition = l_request.partition;

        IF FOUND THEN
            -- Log exists - check if B0 matches
            IF l_existing_log.diskless_start_offset = l_request.diskless_start_offset THEN
                -- Same B0 - idempotent success
                RETURN NEXT (l_request.topic_id, l_request.partition, 'none')::init_diskless_log_response_v1;
            ELSE
                -- Different disklessStartOffset - protocol violation!
                RETURN NEXT (l_request.topic_id, l_request.partition, 'invalid_diskless_start_offset')::init_diskless_log_response_v1;
            END IF;
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
        END IF;
    END LOOP;
END;
$$;
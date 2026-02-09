-- Copyright (c) 2026 Aiven, Helsinki, Finland. https://aiven.io/

ALTER TABLE logs
ADD COLUMN diskless_start_offset offset_t DEFAULT 0;

UPDATE logs
SET diskless_start_offset = 0
WHERE diskless_start_offset IS NULL;

ALTER TABLE logs
ALTER COLUMN diskless_start_offset SET NOT NULL;

CREATE TYPE init_diskless_log_response_error_v1 AS ENUM (
    'none',
    'invalid_request',
    'conflicting_start_offset'
);

CREATE TYPE init_diskless_log_request_v1 AS (
    topic_id topic_id_t,
    partition partition_t,
    diskless_start_offset offset_t
);

CREATE TYPE init_diskless_log_response_v1 AS (
    topic_id topic_id_t,
    partition partition_t,
    error init_diskless_log_response_error_v1,
    diskless_start_offset offset_nullable_t
);

CREATE FUNCTION init_diskless_log_v1(
    arg_requests init_diskless_log_request_v1[]
)
RETURNS SETOF init_diskless_log_response_v1 LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    l_request RECORD;
    l_diskless_start_offset offset_nullable_t;
BEGIN
    IF array_length(arg_requests, 1) IS NULL THEN
        RETURN;
    END IF;

    FOR l_request IN
        SELECT *
        FROM unnest(arg_requests)
        ORDER BY topic_id, partition
    LOOP
        SELECT diskless_start_offset
        INTO l_diskless_start_offset
        FROM logs
        WHERE topic_id = l_request.topic_id
            AND partition = l_request.partition
        FOR UPDATE;

        IF NOT FOUND THEN
            INSERT INTO logs (topic_id, partition, topic_name, log_start_offset, high_watermark, byte_size, diskless_start_offset)
            VALUES (l_request.topic_id, l_request.partition, '', 0, 0, 0, l_request.diskless_start_offset);
            RETURN NEXT (l_request.topic_id, l_request.partition, 'none', l_request.diskless_start_offset)::init_diskless_log_response_v1;
            CONTINUE;
        END IF;

        IF l_diskless_start_offset = l_request.diskless_start_offset THEN
            RETURN NEXT (l_request.topic_id, l_request.partition, 'none', l_diskless_start_offset)::init_diskless_log_response_v1;
            CONTINUE;
        END IF;

        RETURN NEXT (l_request.topic_id, l_request.partition, 'conflicting_start_offset', l_diskless_start_offset)::init_diskless_log_response_v1;
    END LOOP;
END;
$$
;

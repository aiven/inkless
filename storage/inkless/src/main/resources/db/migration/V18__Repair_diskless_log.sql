-- Copyright (c) 2026 Aiven, Helsinki, Finland. https://aiven.io/

CREATE TYPE repair_diskless_log_request_v1 AS (
    topic_id topic_id_t,
    partition partition_t,
    topic_name topic_name_t,
    diskless_start_offset offset_t
);

CREATE TYPE repair_diskless_log_response_v1 AS (
    topic_id topic_id_t,
    partition partition_t,
    found BOOLEAN
);

CREATE OR REPLACE FUNCTION repair_diskless_log_v1(
    arg_requests repair_diskless_log_request_v1[]
)
RETURNS SETOF repair_diskless_log_response_v1 LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    l_request RECORD;
    l_found BOOLEAN;
BEGIN
    FOR l_request IN
        SELECT *
        FROM unnest(arg_requests)
        -- Order to avoid deadlocks with concurrent multi-row updates.
        ORDER BY topic_id, partition
    LOOP
        UPDATE logs
            SET diskless_start_offset = l_request.diskless_start_offset
        WHERE topic_id = l_request.topic_id
            AND partition = l_request.partition;

        l_found := FOUND;

        RETURN NEXT (l_request.topic_id, l_request.partition, l_found)::repair_diskless_log_response_v1;
    END LOOP;
END;
$$
;

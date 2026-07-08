-- Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
--
-- Adds tracking of the cross-tier (remote) log start offset for consolidating diskless topics.
--
-- This is the lowest offset still physically readable across the remote + local tiers, as reported
-- by the partition's classic leader (its RemoteLogManager). It is deliberately distinct from
-- log_start_offset: the latter tracks the diskless WAL prune frontier and can be *ahead* of the
-- true cross-tier earliest offset (remote retention runs independently of WAL pruning).
--
-- A NULL value means "no remote tier tracked yet"; callers should fall back to log_start_offset.

ALTER TABLE logs ADD COLUMN remote_log_start_offset offset_nullable_t DEFAULT NULL;

CREATE TYPE advance_cross_tier_log_start_request_v1 AS (
    topic_id topic_id_t,
    partition partition_t,
    remote_log_start_offset offset_t
);

CREATE TYPE advance_cross_tier_log_start_response_error_v1 AS ENUM (
    'unknown_topic_or_partition'
);

CREATE TYPE advance_cross_tier_log_start_response_v1 AS (
    topic_id topic_id_t,
    partition partition_t,
    error advance_cross_tier_log_start_response_error_v1,
    remote_log_start_offset offset_nullable_t
);

-- Forward-only update of remote_log_start_offset for the given partitions.
--
-- The reported value only ever advances (remote retention is monotonic), so updates that would move
-- it backwards are ignored. This makes the operation idempotent and safe under concurrent writers
-- and retries. The resulting (possibly unchanged) value is returned per request.
CREATE FUNCTION advance_cross_tier_log_start_v1(
    arg_requests advance_cross_tier_log_start_request_v1[]
)
RETURNS SETOF advance_cross_tier_log_start_response_v1 LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    l_request RECORD;
    l_log logs%ROWTYPE;
BEGIN
    FOR l_request IN
        SELECT *
        FROM unnest(arg_requests)
        ORDER BY topic_id, partition  -- ordering is important to prevent deadlocks
    LOOP
        SELECT *
        FROM logs
        WHERE topic_id = l_request.topic_id
            AND partition = l_request.partition
        INTO l_log
        FOR UPDATE;

        IF NOT FOUND THEN
            RETURN NEXT (l_request.topic_id, l_request.partition, 'unknown_topic_or_partition', NULL)::advance_cross_tier_log_start_response_v1;
            CONTINUE;
        END IF;

        IF l_log.remote_log_start_offset IS NULL OR l_request.remote_log_start_offset > l_log.remote_log_start_offset THEN
            UPDATE logs
            SET remote_log_start_offset = l_request.remote_log_start_offset
            WHERE topic_id = l_request.topic_id
                AND partition = l_request.partition;
            l_log.remote_log_start_offset = l_request.remote_log_start_offset;
        END IF;

        RETURN NEXT (
            l_request.topic_id,
            l_request.partition,
            NULL::advance_cross_tier_log_start_response_error_v1,
            l_log.remote_log_start_offset
        )::advance_cross_tier_log_start_response_v1;
    END LOOP;
END;
$$
;

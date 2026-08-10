-- Copyright (c) 2026 Aiven, Helsinki, Finland. https://aiven.io/

-- KC-387: make the classic-to-diskless seal write authoritative over an empty placeholder row.
--
-- A partition-count increase committed during the switch is routed through createDisklessPartitions,
-- which bulk-inserts a logs row for the switching partition at high_watermark = 0 (an unwritten
-- placeholder). The previous init_diskless_log_v1 then hit ON CONFLICT DO NOTHING and returned
-- 'already_initialized', so the seal was dropped and the diskless high_watermark stayed at 0. A
-- diskless LATEST of 0 then made the consolidation fetcher truncate the classic prefix away, losing it.
--
-- initDisklessLog now wins whenever the conflicting row is still a fresh placeholder
-- (high_watermark = 0, diskless_start_offset = 0, byte_size = 0): the guarded upsert overwrites it with
-- the sealed offsets regardless of which writer got there first. A row that already carries diskless
-- data is left untouched and still reports 'already_initialized', so a genuine re-init stays a no-op.
-- Narrowing the create path to the newly added partitions is a best-effort first layer that reduces how
-- often the two writers meet; this guard is what actually decides the outcome when they do.
--
-- Deliberate imprecision: a partition sealed at offset 0 and never written to is indistinguishable from
-- a placeholder, so a later init carrying a different seal would overwrite it, where before this change
-- any existing row won. Reaching that needs a second init with a divergent seal for a partition an
-- earlier leader saw as empty. Distinguishing the two exactly would need the row to record that init
-- wrote it, which is not worth a schema column for that case.

CREATE OR REPLACE FUNCTION init_diskless_log_v1(
    arg_requests init_diskless_log_request_v1[],
    arg_producer_states init_diskless_log_producer_state_v1[]
)
RETURNS SETOF init_diskless_log_response_v1 LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    l_request RECORD;
    l_producer_state RECORD;
BEGIN
    -- Every caller takes existing row locks in the same order; responses still follow arg_requests order.
    PERFORM 1
    FROM logs
    JOIN unnest(arg_requests) AS request
        ON logs.topic_id = request.topic_id
        AND logs.partition = request.partition
    ORDER BY logs.topic_id, logs.partition
    FOR UPDATE OF logs;

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
        ON CONFLICT (topic_id, partition) DO UPDATE
            SET log_start_offset      = EXCLUDED.log_start_offset,
                high_watermark        = EXCLUDED.high_watermark,
                diskless_start_offset = EXCLUDED.diskless_start_offset
            WHERE logs.high_watermark = 0
                AND logs.diskless_start_offset = 0
                AND logs.byte_size = 0
                -- A zero-offset re-init would not advance the placeholder, so leaving it alone keeps
                -- re-initing an existing empty partition a no-op.
                AND EXCLUDED.high_watermark > 0;

        IF NOT FOUND THEN
            RETURN NEXT (l_request.topic_id, l_request.partition, 'already_initialized')::init_diskless_log_response_v1;
            CONTINUE;
        END IF;

        -- The update path can reach a row an earlier init already wrote producer state for, and
        -- producer_state is keyed on a BIGSERIAL row_id, so those rows would accumulate rather than be
        -- replaced. The incoming snapshot is the authority for the partition.
        DELETE FROM producer_state
        WHERE topic_id = l_request.topic_id
            AND partition = l_request.partition;

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

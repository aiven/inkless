-- Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
-- RLMM migrations form an isolated Flyway stream (locations db/rlmm/migration,
-- schema inkless_rlmm, dedicated history table) both at codegen time (the
-- generateRlmmJooqClasses task) and at runtime (see RlmmMigrations).
CREATE SCHEMA IF NOT EXISTS inkless_rlmm;

-- Segment lifecycle states mirror RemoteLogSegmentState ids:
-- 0 COPY_SEGMENT_STARTED, 1 COPY_SEGMENT_FINISHED,
-- 2 DELETE_SEGMENT_STARTED, 3 DELETE_SEGMENT_FINISHED.
-- State 3 rows are tombstones: reads exclude them, and retries of the terminal
-- update complete as no-ops instead of resurrecting the segment.
-- Partition delete states mirror RemotePartitionDeleteState ids:
-- 0 DELETE_PARTITION_MARKED, 1 DELETE_PARTITION_STARTED, 2 DELETE_PARTITION_FINISHED.
CREATE TABLE inkless_rlmm.rlmm_partitions (
    cluster_id TEXT NOT NULL,
    topic_id UUID NOT NULL,
    partition_id INTEGER NOT NULL,
    delete_state SMALLINT NULL CHECK (delete_state IS NULL OR (delete_state >= 0 AND delete_state <= 2)),
    delete_event_timestamp_ms BIGINT NULL,
    delete_broker_id INTEGER NULL,
    revision BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (cluster_id, topic_id, partition_id)
);

CREATE TABLE inkless_rlmm.rlmm_segments (
    cluster_id TEXT NOT NULL,
    topic_id UUID NOT NULL,
    partition_id INTEGER NOT NULL,
    segment_id UUID NOT NULL,
    start_offset BIGINT NOT NULL CHECK (start_offset >= 0),
    end_offset BIGINT NOT NULL CHECK (end_offset >= start_offset),
    max_timestamp_ms BIGINT NOT NULL,
    segment_size INTEGER NOT NULL CHECK (segment_size >= 0),
    txn_idx_empty BOOLEAN NOT NULL,
    -- The segment's original epoch map as a JSON object of epoch to start
    -- offset, mirroring idToSegmentMetadata. Reads materialize from it so a
    -- shadowed segment keeps its lineage after its slot row is replaced.
    leader_epochs JSONB NOT NULL,
    state SMALLINT NOT NULL CHECK (state >= 0 AND state <= 3),
    custom_metadata BYTEA NULL,
    broker_id INTEGER NOT NULL,
    event_timestamp_ms BIGINT NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    PRIMARY KEY (cluster_id, topic_id, partition_id, segment_id),
    CONSTRAINT fk_rlmm_segments_partition FOREIGN KEY (cluster_id, topic_id, partition_id)
        REFERENCES inkless_rlmm.rlmm_partitions (cluster_id, topic_id, partition_id)
        ON DELETE NO ACTION ON UPDATE CASCADE
);
CREATE INDEX rlmm_segments_by_partition_active_idx
    ON inkless_rlmm.rlmm_segments (cluster_id, topic_id, partition_id)
    WHERE state <> 3;

-- One row per segment and leader epoch. referenced mirrors offsetToId membership
-- in RemoteLogLeaderEpochState: the floor lookup only sees referenced rows.
-- Unreferenced rows stay for listSegments cleanup enumeration, matching the
-- unreferencedSegmentIds set. A stale same-start finisher silently replaces
-- the slot (the victim row is deleted), matching offsetToId.put semantics.
CREATE TABLE inkless_rlmm.rlmm_segment_epochs (
    cluster_id TEXT NOT NULL,
    topic_id UUID NOT NULL,
    partition_id INTEGER NOT NULL,
    segment_id UUID NOT NULL,
    epoch INTEGER NOT NULL,
    epoch_start_offset BIGINT NOT NULL,
    epoch_end_offset BIGINT NOT NULL,
    referenced BOOLEAN NOT NULL,
    PRIMARY KEY (cluster_id, topic_id, partition_id, segment_id, epoch),
    CONSTRAINT fk_rlmm_segment_epochs_segment FOREIGN KEY (cluster_id, topic_id, partition_id, segment_id)
        REFERENCES inkless_rlmm.rlmm_segments (cluster_id, topic_id, partition_id, segment_id)
        ON DELETE CASCADE ON UPDATE CASCADE
);
-- One referenced segment per (partition, epoch, epoch start offset).
CREATE UNIQUE INDEX rlmm_segment_epochs_referenced_uniq_idx
    ON inkless_rlmm.rlmm_segment_epochs (cluster_id, topic_id, partition_id, epoch, epoch_start_offset)
    WHERE referenced;
CREATE INDEX rlmm_segment_epochs_floor_idx
    ON inkless_rlmm.rlmm_segment_epochs (cluster_id, topic_id, partition_id, epoch, epoch_start_offset)
    WHERE referenced;

-- Durable per-epoch high watermark. It never decreases, including on deletion,
-- matching RemoteLogLeaderEpochState.highestLogOffset.
CREATE TABLE inkless_rlmm.rlmm_epoch_state (
    cluster_id TEXT NOT NULL,
    topic_id UUID NOT NULL,
    partition_id INTEGER NOT NULL,
    epoch INTEGER NOT NULL,
    highest_offset BIGINT NOT NULL,
    PRIMARY KEY (cluster_id, topic_id, partition_id, epoch),
    CONSTRAINT fk_rlmm_epoch_state_partition FOREIGN KEY (cluster_id, topic_id, partition_id)
        REFERENCES inkless_rlmm.rlmm_partitions (cluster_id, topic_id, partition_id)
        ON DELETE NO ACTION ON UPDATE CASCADE
);

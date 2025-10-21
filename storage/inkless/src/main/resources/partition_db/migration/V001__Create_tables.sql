-- Copyright (c) 2024-2025 Aiven, Helsinki, Finland. https://aiven.io/

CREATE TABLE IF NOT EXISTS logs (
    topic_id TEXT NOT NULL,  -- TODO replace with more compact ID
    partition INTEGER NOT NULL,
    topic_name TEXT NOT NULL,
    log_start_offset INTEGER NOT NULL,
    high_watermark INTEGER NOT NULL,
    byte_size INTEGER NOT NULL,
    PRIMARY KEY(topic_id, partition)
);

CREATE TABLE IF NOT EXISTS files (
    file_id INTEGER PRIMARY KEY AUTOINCREMENT,
    object_key TEXT UNIQUE NOT NULL,
    format TEXT NOT NULL,  -- TODO more compact
    reason TEXT NOT NULL,  -- TODO more compact
    state TEXT NOT NULL,  -- TODO more compact
    uploader_broker_id INTEGER NOT NULL,
    committed_at INTEGER NOT NULL,
    marked_for_deletion_at INTEGER,
    size INTEGER NOT NULL
);

CREATE INDEX files_by_state_only_deleting_idx ON files (state) WHERE state = 'deleting';

CREATE TABLE IF NOT EXISTS batches (
    batch_id INTEGER PRIMARY KEY AUTOINCREMENT,
    magic INTEGER NOT NULL,
    topic_id TEXT NOT NULL,  -- TODO more compact
    partition INTEGER NOT NULL,
    base_offset INTEGER NOT NULL,
    last_offset INTEGER NOT NULL,
    file_id INTEGER NOT NULL,
    byte_offset INTEGER NOT NULL,
    byte_size INTEGER NOT NULL,
    timestamp_type INTEGER NOT NULL,
    log_append_timestamp INTEGER NOT NULL,
    batch_max_timestamp INTEGER NOT NULL,
    FOREIGN KEY (topic_id, partition) REFERENCES logs(topic_id, partition),
    FOREIGN KEY (file_id) REFERENCES files(file_id)
);

CREATE INDEX batches_by_last_offset_idx ON batches (last_offset);
CREATE INDEX batches_by_file ON batches (file_id);

CREATE TABLE IF NOT EXISTS producer_state (
    topic_id TEXT NOT NULL,  -- TODO more compact
    partition INTEGER NOT NULL,
    producer_id INTEGER NOT NULL,
    row_id INTEGER PRIMARY KEY AUTOINCREMENT,
    producer_epoch INTEGER NOT NULL,
    base_sequence INTEGER NOT NULL,
    last_sequence INTEGER NOT NULL,
    assigned_offset INTEGER NOT NULL,
    batch_max_timestamp INTEGER NOT NULL
);

CREATE INDEX producer_state_by_topic_id_partition_producer_id ON producer_state (topic_id, partition, producer_id);

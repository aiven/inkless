-- Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/

CREATE TABLE logs (
    topic_id UUID NOT NULL,
    partition INT NOT NULL CHECK (partition >= 0),
    topic_name VARCHAR(255) NOT NULL,
    log_start_offset BIGINT NOT NULL CHECK (log_start_offset >= 0),
    high_watermark BIGINT NOT NULL CHECK (high_watermark >= 0),
    PRIMARY KEY(topic_id, partition)
);

CREATE TABLE batches (
    topic_id UUID NOT NULL,
    partition INT NOT NULL CHECK (partition >= 0),
    base_offset BIGINT NOT NULL CHECK (base_offset >= 0),
    last_offset BIGINT NOT NULL CHECK (last_offset >= 0),
    object_key VARCHAR(1024) NOT NULL,
    byte_offset BIGINT NOT NULL CHECK (byte_offset >= 0),
    byte_size BIGINT NOT NULL CHECK (byte_size >= 0),  -- TODO replace with INT?
    number_of_records BIGINT NOT NULL CHECK (number_of_records >= 0),  -- TODO replace with INT?
    timestamp_type SMALLINT NOT NULL CHECK (timestamp_type >= -1 AND timestamp_type <= 1),
    log_append_timestamp BIGINT NOT NULL CHECK (log_append_timestamp >= -1),
    batch_max_timestamp BIGINT NOT NULL CHECK (batch_max_timestamp >= -1),
    PRIMARY KEY(topic_id, partition, base_offset)
);

CREATE INDEX batches_by_last_offset_idx ON batches (topic_id, partition, last_offset);

-- Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/

-- Create index on the same columns as batches_by_last_offset_idx, but including also other
-- columns that are useful to speed up the scans done by RetentionEnforcer.
CREATE INDEX batches_by_last_offset_covering_idx ON batches (topic_id, partition, last_offset)
    INCLUDE (base_offset, byte_size, timestamp_type, batch_max_timestamp, log_append_timestamp);

-- This index now is a duplicate and it's not needed anymore.
DROP INDEX batches_by_last_offset_idx;

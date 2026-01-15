-- Copyright (c) 2024-2025 Aiven, Helsinki, Finland. https://aiven.io/
ALTER TABLE logs ADD COLUMN diskless_start_offset offset_nullable_t DEFAULT NULL;
ALTER TABLE logs ADD COLUMN diskless_end_offset offset_nullable_t DEFAULT NULL;
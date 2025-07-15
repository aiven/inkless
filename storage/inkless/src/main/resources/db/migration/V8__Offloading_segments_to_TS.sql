-- Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/

ALTER TABLE logs
ADD COLUMN ts_high_watermark offset_t DEFAULT 0;

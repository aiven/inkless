-- Copyright (c) 2026 Aiven, Helsinki, Finland. https://aiven.io/

-- No caller uses this function: the topic switch tool no longer has a `repair` command.
DROP FUNCTION IF EXISTS repair_diskless_log_v1 CASCADE;

DROP TYPE IF EXISTS repair_diskless_log_response_v1 CASCADE;
DROP TYPE IF EXISTS repair_diskless_log_request_v1 CASCADE;

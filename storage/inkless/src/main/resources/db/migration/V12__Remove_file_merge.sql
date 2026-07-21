-- Remove file merge infrastructure

-- Drop functions (CASCADE handles dependent types)
DROP FUNCTION IF EXISTS release_file_merge_work_item_v1 CASCADE;
DROP FUNCTION IF EXISTS commit_file_merge_work_item_v1 CASCADE;
DROP FUNCTION IF EXISTS get_file_merge_work_item_v1 CASCADE;

-- Drop tables
DROP TABLE IF EXISTS file_merge_work_item_files;
DROP TABLE IF EXISTS file_merge_work_items;

-- Drop types (CASCADE handles any remaining dependencies)
DROP TYPE IF EXISTS release_file_merge_work_item_response_v1 CASCADE;
DROP TYPE IF EXISTS release_file_merge_work_item_error_v1 CASCADE;
DROP TYPE IF EXISTS commit_file_merge_work_item_response_v1 CASCADE;
DROP TYPE IF EXISTS commit_file_merge_work_item_error_v1 CASCADE;
DROP TYPE IF EXISTS commit_file_merge_work_item_batch_v1 CASCADE;
DROP TYPE IF EXISTS file_merge_work_item_response_v1 CASCADE;
DROP TYPE IF EXISTS file_merge_work_item_response_file_v1 CASCADE;
DROP TYPE IF EXISTS file_merge_work_item_response_batch_v1 CASCADE;

-- Replace delete_files_v1 to remove the file_merge_work_item_files reference
CREATE OR REPLACE FUNCTION delete_files_v1(
    arg_paths object_key_t[]
)
RETURNS VOID LANGUAGE plpgsql VOLATILE AS $$
BEGIN
    DELETE FROM files
    WHERE object_key = ANY(arg_paths)
        AND state = 'deleting';
END;
$$
;

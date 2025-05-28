CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

DO $$
BEGIN
  RAISE NOTICE 'pg_stat_statements extension creation attempted.';
END $$;
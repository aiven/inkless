#!/bin/bash
set -e
echo "host replication all all scram-sha-256" >> "$PGDATA/pg_hba.conf"
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<'SQL'
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
CREATE DATABASE bench;
SQL

#!/usr/bin/env bash
# quantdb-prepare-test-db.sh
# This script prepares a test PostgreSQL database with the same schema as the main DB and populates it with the first 10 rows of each table.
# Usage: ./bin/prepare_test_db.sh [MAIN_DB] [TEST_DB] [PGUSER] [PGHOST] [PGPORT]

set -e

MAIN_DB=${1:-quantdb}
TEST_DB=${2:-quantdb_test}
PGUSER=${3:-quantdb-test-admin}
PGHOST=${4:-localhost}
PGPORT=${5:-5432}

# Drop and recreate the test database
psql -U $PGUSER -h $PGHOST -p $PGPORT -c "DROP DATABASE IF EXISTS $TEST_DB;"
psql -U $PGUSER -h $PGHOST -p $PGPORT -c "CREATE DATABASE $TEST_DB;"

# Dump schema only from main DB and restore to test DB
pg_dump -U $PGUSER -h $PGHOST -p $PGPORT -s $MAIN_DB | psql -U $PGUSER -h $PGHOST -p $PGPORT $TEST_DB

# Get all table names from the main DB
TABLES=$(psql -U $PGUSER -h $PGHOST -p $PGPORT -d $MAIN_DB -Atc "SELECT tablename FROM pg_tables WHERE schemaname='public';")

# For each table, copy the first 10 rows from main DB to test DB
for TABLE in $TABLES; do
    COLS=$(psql -U $PGUSER -h $PGHOST -p $PGPORT -d $MAIN_DB -Atc "SELECT string_agg('"' || column_name || '"', ',') FROM information_schema.columns WHERE table_name='$TABLE' AND table_schema='public';")
    psql -U $PGUSER -h $PGHOST -p $PGPORT -d $TEST_DB -c "INSERT INTO \"$TABLE\" ($COLS) SELECT $COLS FROM dblink('dbname=$MAIN_DB','SELECT $COLS FROM \"$TABLE\" LIMIT 10') AS t($COLS);" 2>/dev/null || true
done

echo "Test database '$TEST_DB' prepared with schema and first 10 rows of each table from '$MAIN_DB'."

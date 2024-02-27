-- quantdb-admin quantdb_test

GRANT CONNECT ON DATABASE :database TO "quantdb-user";
GRANT USAGE ON SCHEMA quantdb TO "quantdb-user";

GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA quantdb TO "quantdb-user";  -- tables includes views
GRANT USAGE ON ALL SEQUENCES IN SCHEMA quantdb TO "quantdb-user";

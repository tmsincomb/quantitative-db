-- quantdb-admin quantdb_test

GRANT CONNECT ON DATABASE :database TO :"perm_user";
GRANT USAGE ON SCHEMA quantdb TO :"perm_user";

GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA quantdb TO :"perm_user";  -- tables includes views
GRANT USAGE ON ALL SEQUENCES IN SCHEMA quantdb TO :"perm_user";

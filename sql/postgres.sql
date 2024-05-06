-- postgres postgres
-- CONNECT TO postgres USER postgres;

DO
$body$
BEGIN
    IF NOT EXISTS ( SELECT * FROM pg_catalog.pg_user
        WHERE usename = 'quantdb-user') THEN
        CREATE ROLE "quantdb-user" LOGIN
        NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE;
    END IF;
    IF NOT EXISTS ( SELECT * FROM pg_catalog.pg_user
        WHERE usename = 'quantdb-admin') THEN
        CREATE ROLE "quantdb-admin" LOGIN
        NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE;
    END IF;
END;
$body$ language plpgsql;

-- postgres postgres

ALTER ROLE "quantdb-admin" SET search_path = quantdb, public;
ALTER ROLE "quantdb-user" SET search_path = quantdb, public;

-- postgres postgres

-- rds needs this https://stackoverflow.com/a/34898033
GRANT "quantdb-admin" TO CURRENT_USER;

-- postgres postgres

DROP DATABASE IF EXISTS :database;

-- postgres postgres

CREATE DATABASE :database -- quantdb
    WITH OWNER = 'quantdb-admin'
    ENCODING = 'UTF8'
    --TABLESPACE = pg_default  -- leave this out for now since rds doesn't really support it
    LC_COLLATE = 'en_US.UTF-8'  -- this was a gentoo locale issue check ${LANG}
    LC_CTYPE = 'en_US.UTF-8'
    CONNECTION LIMIT = -1;

REVOKE "quantdb-admin" FROM CURRENT_USER;

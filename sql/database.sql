-- postgres postgres
-- CONNECT TO postgres USER postgres;

-- rds needs this https://stackoverflow.com/a/34898033
GRANT "quantdb-admin" TO CURRENT_USER;

-- postgres postgres

CREATE DATABASE :database -- quantdb
    WITH OWNER = 'quantdb-admin'
    ENCODING = 'UTF8'
    --TABLESPACE = pg_default  -- leave this out for now since rds doesn't really support it
    LC_COLLATE = 'en_US.UTF-8'  -- this was a gentoo locale issue check ${LANG}
    LC_CTYPE = 'en_US.UTF-8'
    CONNECTION LIMIT = -1;

REVOKE "quantdb-admin" FROM CURRENT_USER;

/* script for creating the lakehouse staggind database */
DROP DATABASE IF EXISTS rezstage;
CREATE DATABASE rezstage   
 WITH OWNER = postgres
      ENCODING = 'UTF8'
      TABLESPACE = pg_default
      LC_COLLATE = 'en_US.UTF-8'
      LC_CTYPE = 'en_US.UTF-8'
      CONNECTION LIMIT = -1;

COMMENT ON DATABASE rezstage IS 'entry point for stagging all internal and external data in the lakehouse';

/* Lakehouse schema applies to stage, historic, and curated databases */
DROP SCHEMA IF EXISTS lakehouse CASCADE;
CREATE SCHEMA IF NOT EXISTS lakehouse
        AUTHORIZATION postgres;


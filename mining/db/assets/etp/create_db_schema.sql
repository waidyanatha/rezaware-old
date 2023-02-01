/* script for creating the lakehouse staggind database */
DROP DATABASE IF EXISTS tip;
CREATE DATABASE tip
 WITH OWNER = postgres
      ENCODING = 'UTF8'
      TABLESPACE = pg_default
      LC_COLLATE = 'en_US.UTF-8'
      LC_CTYPE = 'en_US.UTF-8'
      CONNECTION LIMIT = -1;

COMMENT ON DATABASE tip IS 'TIP warehouse ROR, SMA, SMSD data';

/* create admin user for tip with all priviledges */
CREATE USER farmraider WITH PASSWORD 'spirittribe';
GRANT ALL PRIVILEGES ON DATABASE "tip" to farmraider;

/* Lakehouse schema applies to stage, historic, and curated databases */
DROP SCHEMA IF EXISTS warehouse CASCADE;
CREATE SCHEMA IF NOT EXISTS warehouse
        AUTHORIZATION farmraider;
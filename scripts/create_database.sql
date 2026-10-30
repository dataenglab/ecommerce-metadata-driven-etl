-- create_database.sql : one-time setup of the warehouse database (run before schema.sql)
--
-- Run as a PostgreSQL superuser, connected to the maintenance database "postgres":
--     psql "host=localhost port=5433 user=postgres dbname=postgres" -f scripts/create_database.sql
--
-- The engine does not create the database itself. It connects to an existing database
-- whose connection string is read from the environment variable named in
-- config/pipeline_config.yaml (pipeline.database.dsn_env = ETL_PG_DSN), for example:
--     export ETL_PG_DSN="host=localhost port=5433 dbname=shop user=etl password=<password>"
-- ETL_PG_DSN is therefore the name of an environment variable, not of a database.

CREATE ROLE etl LOGIN PASSWORD 'change_me';      -- application user of the pipeline
CREATE DATABASE shop OWNER etl;                   -- warehouse database: schemas etl and dwh

-- Next step: create the schemas and tables inside the new database:
--     psql "host=localhost port=5433 dbname=shop user=etl password=change_me" -f scripts/schema.sql

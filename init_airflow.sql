SELECT 'CREATE DATABASE airflow_db' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow_db')\gexec
DO $$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_catalog.pg_user WHERE usename = 'airflow_user') THEN
    CREATE USER airflow_user WITH PASSWORD 'airflow_pass';
  END IF;
END $$;
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow_user;
ALTER USER airflow_user CREATEDB SUPERUSER;
\c airflow_db
GRANT ALL ON SCHEMA public TO airflow_user;

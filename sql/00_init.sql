CREATE EXTENSION IF NOT EXISTS timescaledb;
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grafana_reader') THEN
    CREATE ROLE grafana_reader LOGIN PASSWORD 'grafana';
  END IF;
END $$;
GRANT pg_read_all_data TO grafana_reader;

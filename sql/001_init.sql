-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Main table
CREATE TABLE IF NOT EXISTS measurements (
  id               bigserial PRIMARY KEY,
  device_id        text NOT NULL,
  ts               timestamptz NOT NULL,
  temp_c           real,
  hum_pct          real,
  raw_payload      jsonb
);

-- Convert to a hypertable (time + optional device partition)
SELECT create_hypertable('measurements', 'ts', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

-- Automatic retention (keep only the last 30 days)
SELECT add_retention_policy('measurements', INTERVAL '30 days');

-- (Optional) compression of chunks older than 7 days
ALTER TABLE measurements SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'device_id'
);
SELECT add_compression_policy('measurements', INTERVAL '7 days');

-- (Optional) continuous aggregate for hourly averages
CREATE MATERIALIZED VIEW measurements_hourly_avg
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 hour', ts) AS bucket,
  device_id,
  avg(temp_c) AS avg_temp_c,
  avg(hum_pct) AS avg_hum_pct
FROM measurements
GROUP BY bucket, device_id;

SELECT add_continuous_aggregate_policy('measurements_hourly_avg',
    start_offset => INTERVAL '7 days',
    end_offset   => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');

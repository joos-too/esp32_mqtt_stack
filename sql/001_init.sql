CREATE TABLE IF NOT EXISTS measurements (
  id               bigserial PRIMARY KEY,
  device_id        text NOT NULL,
  ts               timestamptz NOT NULL DEFAULT now(),
  temp_c           real,
  hum_pct          real,
  raw_payload      jsonb
);

CREATE INDEX IF NOT EXISTS idx_measurements_ts ON measurements(ts);
CREATE INDEX IF NOT EXISTS idx_measurements_device_ts ON measurements(device_id, ts DESC);
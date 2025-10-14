CREATE TABLE IF NOT EXISTS measurements (
  id               bigserial PRIMARY KEY,
  device_id        text NOT NULL,
  ts               timestamptz NOT NULL DEFAULT now(),
  temp_c           real,
  hum_pct          real,
  cpu_total_pct    real,
  cpu_mp_pct       real,
  core0_cpu_pct    real,
  core1_cpu_pct    real,
  mp_used_kb       integer,
  mp_total_kb      integer,
  idf_free_kb      integer,
  raw_payload      jsonb
);

CREATE INDEX IF NOT EXISTS idx_measurements_ts ON measurements(ts);
CREATE INDEX IF NOT EXISTS idx_measurements_device_ts ON measurements(device_id, ts DESC);
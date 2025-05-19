-- migrations/schema.sql

-- 1) Raw appointments: store exactly the original CSV columns (no derived features)
CREATE TABLE IF NOT EXISTS raw_appointments (
  patient_id       BIGINT       NOT NULL,
  appointment_id   BIGINT       PRIMARY KEY,
  gender           VARCHAR(10),
  scheduled_day    TIMESTAMP    NOT NULL,
  appointment_day  DATE         NOT NULL,
  age              INTEGER      NOT NULL,
  neighbourhood    VARCHAR(100),
  scholarship      BOOLEAN      NOT NULL,
  hypertension     BOOLEAN      NOT NULL,
  diabetes         BOOLEAN      NOT NULL,
  alcoholism       BOOLEAN      NOT NULL,
  handicap         INTEGER      NOT NULL,
  sms_received     BOOLEAN      NOT NULL,
  no_show          BOOLEAN      NOT NULL
);

-- 2) Processed appointments: same columns, plus derived features
CREATE TABLE IF NOT EXISTS processed_appointments (
  patient_id           BIGINT       NOT NULL,
  appointment_id       BIGINT       PRIMARY KEY,
  gender               VARCHAR(10),
  scheduled_day        TIMESTAMP    NOT NULL,
  appointment_day      DATE         NOT NULL,
  age                  INTEGER      NOT NULL,
  neighbourhood        VARCHAR(100),
  scholarship          BOOLEAN      NOT NULL,
  hypertension         BOOLEAN      NOT NULL,
  diabetes             BOOLEAN      NOT NULL,
  alcoholism           BOOLEAN      NOT NULL,
  handicap             INTEGER      NOT NULL,
  sms_received         BOOLEAN      NOT NULL,
  no_show              BOOLEAN      NOT NULL,

  -- derived features added by cleaning_utils.py
  wait_days            INTEGER,       -- days between scheduling & appointment
  scheduled_hour       INTEGER,       -- hour of day (0–23) from scheduled_day
  appointment_weekday  INTEGER,       -- 0=Monday … 6=Sunday
  age_group            VARCHAR(20)    -- e.g. "child", "adult", "senior"
);

CREATE TABLE IF NOT EXISTS test_appointments (
  LIKE processed_appointments INCLUDING ALL
);
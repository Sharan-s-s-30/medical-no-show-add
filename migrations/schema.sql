-- migrations/schema.sql

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


CREATE TABLE IF NOT EXISTS processed_appointments(
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

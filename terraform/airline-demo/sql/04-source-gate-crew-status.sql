-- Source topic: ops operational system (CDC). Spoke-local: the ops spoke joins
-- this downstream for feasibility (can we hold? is another gate free?).
-- Keyed by flight_id (`key`, the onward flight).
CREATE TABLE IF NOT EXISTS gate_crew_status (
  `key` STRING NOT NULL,
  crew_duty_margin_min INT,
  alt_gate STRING,
  bag_team_available BOOLEAN,
  PRIMARY KEY (`key`) NOT ENFORCED
) DISTRIBUTED BY (`key`) INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'upsert',
  -- Confluent raw key format requires the single STRING PK column to be named
  -- `key`; the Python datagen writes flight_id as a plain StringSerializer key.
  'key.format' = 'raw',
  'value.format' = 'avro-registry'
);

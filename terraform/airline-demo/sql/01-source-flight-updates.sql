-- Source topic: flight operations system.
-- The trigger: a slipped inbound arrival cascades into the connection.
-- Upsert-keyed by flight_id (`key`) so the datagen "slip" (a re-produced JA417 row with a
-- later estimated_arrival) updates in place rather than appending.
CREATE TABLE IF NOT EXISTS flight_updates (
  `key` STRING NOT NULL,
  route STRING,
  estimated_arrival TIMESTAMP(3),
  estimated_departure TIMESTAMP(3),
  gate STRING,
  PRIMARY KEY (`key`) NOT ENFORCED
) DISTRIBUTED BY (`key`) INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'upsert',
  -- Confluent raw key format requires the single STRING PK column to be named
  -- `key`; the Python datagen writes the flight_id as a plain StringSerializer key.
  'key.format' = 'raw',
  'value.format' = 'avro-registry'
);

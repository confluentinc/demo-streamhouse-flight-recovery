-- Source topic: reservations system. One row per passenger's journey.
-- Upsert-keyed by passenger_id (`key`).
CREATE TABLE IF NOT EXISTS passenger_itineraries (
  `key` STRING NOT NULL,
  passenger_name STRING,
  inbound_flight STRING,
  connecting_flight STRING,
  destination STRING,
  checked_bags INT,
  international BOOLEAN,
  PRIMARY KEY (`key`) NOT ENFORCED
) DISTRIBUTED BY (`key`) INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'upsert',
  -- Confluent raw key format requires the single STRING PK column to be named
  -- `key`; the Python datagen writes passenger_id as a plain StringSerializer key.
  'key.format' = 'raw',
  'value.format' = 'avro-registry'
);

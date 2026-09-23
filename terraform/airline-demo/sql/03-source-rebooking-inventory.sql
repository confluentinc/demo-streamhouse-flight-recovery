-- Source topic: recovery-resources system. Spoke-local: the concierge streaming
-- agent joins this downstream to build a plan. Keyed by destination (`key`).
CREATE TABLE IF NOT EXISTS rebooking_inventory (
  `key` STRING NOT NULL,
  next_flight STRING,
  next_departure TIMESTAMP(3),
  hotel STRING,
  PRIMARY KEY (`key`) NOT ENFORCED
) DISTRIBUTED BY (`key`) INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'upsert',
  -- Confluent raw key format requires the single STRING PK column to be named
  -- `key`; the Python datagen writes destination as a plain StringSerializer key.
  'key.format' = 'raw',
  'value.format' = 'avro-registry'
);

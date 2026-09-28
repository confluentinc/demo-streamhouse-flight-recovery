CREATE TABLE IF NOT EXISTS passenger_recommendations (
  `key` STRING NOT NULL,
  passenger_id STRING,
  recommended_flight_id STRING,
  hotel_id STRING,
  status STRING,
  hotel_cost DECIMAL(10, 2),
  recommended_at TIMESTAMP(3),
  recovered_at TIMESTAMP(3),
  PRIMARY KEY (`key`) NOT ENFORCED
) DISTRIBUTED BY (`key`) INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'upsert',
  'key.format' = 'raw',
  'kafka.cleanup-policy' = 'compact',
  'value.format' = 'avro-registry'
);

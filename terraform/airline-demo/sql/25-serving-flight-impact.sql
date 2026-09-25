CREATE OR ALTER MATERIALIZED TABLE flight_impact (
  `key` STRING NOT NULL,
  affected_passengers INT,
  PRIMARY KEY (`key`) NOT ENFORCED
) DISTRIBUTED BY (`key`) INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'upsert',
  'key.format' = 'raw',
  'kafka.cleanup-policy' = 'compact',
  'value.format' = 'avro-registry'
)
START_MODE = FROM_BEGINNING
AS SELECT
  COALESCE(inbound_flight_id, '') AS `key`,
  CAST(COUNT(*) FILTER (WHERE risk = 'HIGH') AS INT) AS affected_passengers
FROM passenger_risk
GROUP BY inbound_flight_id;

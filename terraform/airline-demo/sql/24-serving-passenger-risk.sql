CREATE OR ALTER MATERIALIZED TABLE passenger_risk (
  `key` STRING NOT NULL,
  inbound_flight_id STRING,
  connecting_flight_id STRING,
  connection_minutes INT,
  risk STRING,
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
  p.`key`,
  p.inbound_flight_id,
  p.connecting_flight_id,
  TIMESTAMPDIFF(MINUTE, inbound.estimated_time, onward.estimated_time) AS connection_minutes,
  CASE
    WHEN TIMESTAMPDIFF(MINUTE, inbound.estimated_time, onward.estimated_time) < 45 THEN 'HIGH'
    ELSE 'OK'
  END AS risk
FROM passenger_connections p
JOIN flight_status inbound ON p.inbound_flight_id = inbound.`key`
JOIN flight_status onward ON p.connecting_flight_id = onward.`key`;

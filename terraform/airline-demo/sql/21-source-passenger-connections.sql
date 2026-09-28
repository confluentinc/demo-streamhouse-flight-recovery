-- Three-column connection feed; the legacy passenger_itineraries topic has seven columns.
CREATE TABLE IF NOT EXISTS passenger_connections (
  `key` STRING NOT NULL,
  inbound_flight_id STRING,
  connecting_flight_id STRING,
  PRIMARY KEY (`key`) NOT ENFORCED
) DISTRIBUTED BY (`key`) INTO 1 BUCKETS
WITH ('changelog.mode' = 'upsert', 'key.format' = 'raw', 'value.format' = 'avro-registry');

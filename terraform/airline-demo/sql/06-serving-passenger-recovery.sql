-- Streaming-agent output. key = passenger_id. RTCE -> external passenger agent;
-- also to the care desk. raw key + compaction is the Lightning/RTCE serving contract.
CREATE TABLE IF NOT EXISTS passenger_recovery (
  `key` STRING NOT NULL,
  recovery_type STRING,
  action STRING,
  status STRING,
  PRIMARY KEY (`key`) NOT ENFORCED
) DISTRIBUTED BY (`key`) INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'upsert',
  'key.format' = 'raw',
  'kafka.cleanup-policy' = 'compact',
  'value.format' = 'avro-registry'
);

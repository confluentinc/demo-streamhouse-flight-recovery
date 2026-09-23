-- THE central governed table, as a MATERIALIZED TABLE: one persistent object that
-- owns BOTH the table definition and the continuous query (replacing the old
-- CREATE TABLE + separate INSERT INTO pair). Evolve it in place — e.g. to add a
-- column — by re-running this CREATE OR ALTER: Flink stops the old query and starts
-- the new one writing to the SAME backing topic, so the Lightning/RTCE/Tableflow
-- consumers downstream are unaffected (no topic swap, no consumer migration).
--
-- Every consumer reads this table: Lightning -> ops app + passenger app; RTCE ->
-- external agent; Tableflow -> analytics; and the concierge agent reads it before
-- joining rebooking_inventory. raw key + upsert + compaction is the Lightning/RTCE
-- serving contract; we declare changelog.mode = upsert + PRIMARY KEY explicitly
-- because a JOIN would otherwise default to a retract changelog.
--
-- The two joins to flight_updates (inbound arrival + connecting departure/gate) are
-- the whole story; risk is legible without ML. Sources are keyed by natural ids, so
-- the join output is an upsert stream keyed by passenger_id matching the PK.
-- START_MODE = FROM_BEGINNING reprocesses the (small, deterministic) source topics
-- on create/evolve, so every passenger row carries the current schema immediately.
CREATE OR ALTER MATERIALIZED TABLE passenger_journey (
  `key` STRING NOT NULL,
  passenger_name STRING,
  connecting_flight STRING,
  destination STRING,
  gate STRING,
  make_connection BOOLEAN,
  minutes_to_departure INT,
  bag_status STRING,
  needs_recheck BOOLEAN,
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
AS
SELECT
  p.`key`,
  p.passenger_name,
  p.connecting_flight,
  p.destination,
  c.gate,
  (inb.estimated_arrival + INTERVAL '30' MINUTE <= c.estimated_departure) AS make_connection,
  TIMESTAMPDIFF(MINUTE, inb.estimated_arrival, c.estimated_departure) AS minutes_to_departure,
  CASE WHEN p.checked_bags > 0 THEN 'CHECKED' ELSE 'CARRY_ON' END AS bag_status,
  (p.international AND p.checked_bags > 0) AS needs_recheck,
  CASE
    WHEN inb.estimated_arrival + INTERVAL '30' MINUTE > c.estimated_departure THEN 'MISS'
    WHEN TIMESTAMPDIFF(MINUTE, inb.estimated_arrival, c.estimated_departure) < 45 THEN 'TIGHT'
    ELSE 'OK'
  END AS risk
FROM passenger_itineraries p
JOIN flight_updates inb ON p.inbound_flight = inb.`key`
JOIN flight_updates c   ON p.connecting_flight = c.`key`;

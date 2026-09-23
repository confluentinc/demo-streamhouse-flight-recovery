-- Ops-spoke output, as a MATERIALIZED TABLE (definition + continuous query in one
-- object, replacing the old CREATE TABLE + INSERT INTO pair; evolve in place with
-- CREATE OR ALTER). key = connecting_flight. Lightning -> airline ops app.
--
-- One row per onward departure: connection demand (aggregated from the central
-- table) meets ops feasibility (joined from gate_crew_status). Keeps ops a consumer
-- of the one central table -- connections_at_risk is the same governed rows the
-- concierge acts on. The GROUP BY re-keys the aggregate to connecting_flight and
-- yields an upsert changelog; we still declare the raw-key + compaction serving
-- contract explicitly. START_MODE = FROM_BEGINNING reprocesses the deterministic
-- source on create/evolve.
CREATE OR ALTER MATERIALIZED TABLE flight_ops_state (
  `key` STRING NOT NULL,
  destination STRING,
  gate STRING,
  connections_at_risk INT,
  connections_total INT,
  min_headroom INT,
  recommended_action STRING,
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
  COALESCE(j.connecting_flight, '') AS `key`,  -- COALESCE makes the GROUP BY key NOT NULL (upsert PK requires it)
  MAX(j.destination)                                              AS destination,
  MAX(j.gate)                                                     AS gate,
  CAST(COUNT(*) FILTER (WHERE j.risk IN ('MISS','TIGHT')) AS INT) AS connections_at_risk,
  CAST(COUNT(*) AS INT)                                           AS connections_total,
  MIN(j.minutes_to_departure)                                     AS min_headroom,
  CASE
    WHEN COUNT(*) FILTER (WHERE j.risk IN ('MISS','TIGHT')) = 0 THEN 'NONE'
    WHEN MAX(g.crew_duty_margin_min) >= 15 THEN 'HOLD'         -- crew can wait -> hold the departure
    WHEN MAX(g.alt_gate) IS NOT NULL       THEN 'REGATE'       -- move to a closer/free gate
    ELSE 'PRIORITY_BAGS'                                       -- at least expedite the bags
  END AS recommended_action
FROM passenger_journey j
LEFT JOIN gate_crew_status g ON j.connecting_flight = g.`key`
GROUP BY j.connecting_flight;

-- Runs the recovery agent once per passenger, the first time their connection turns HIGH risk
-- on the live day, and writes its two offers to passenger_recommendations.
--
-- first_high groups the append-only change rows by passenger. TO_CHANGELOG forwards only
-- each group's first row (INSERT), so later updates to an already-HIGH passenger (another
-- delay notice) don't run the agent again. impacted_at is when that first HIGH row landed.
-- History days keep the generator's offers, so only today's flights are sent to the agent.
-- AI_RUN_AGENT can't run inside a materialized table, hence CREATE TABLE plus this INSERT.
INSERT INTO passenger_recommendations
WITH first_high AS (
  SELECT
    `key`,
    inbound_flight_id,
    connecting_flight_id,
    final_destination,
    CAST(MIN($rowtime) AS TIMESTAMP(3)) AS impacted_at
  FROM passenger_state_changes
  WHERE risk = 'HIGH'
    AND RIGHT(inbound_flight_id, 8) >= DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyyMMdd')
  GROUP BY `key`, inbound_flight_id, connecting_flight_id, final_destination
),
newly_high AS (
  SELECT *
  FROM TO_CHANGELOG(
    input      => TABLE first_high PARTITION BY `key`,
    op_mapping => MAP['INSERT', 'FIRST_HIGH']
  )
),
plans AS (
  SELECT h.`key` AS passenger_id, h.impacted_at, r.response
  FROM newly_high h,
  LATERAL TABLE(AI_RUN_AGENT(
    'passenger_recovery_agent',
    CONCAT(
      'passenger_id=', h.`key`,
      '; inbound_flight_id=', h.inbound_flight_id,
      '; missed_connection=', h.connecting_flight_id,
      '; final_destination=', h.final_destination,
      '; inbound_query=SELECT * FROM `flight_status` WHERE "KEY" = ''', h.inbound_flight_id, ''' LIMIT 1',
      '; departures_query=SELECT * FROM `flight_status` WHERE origin = ''SFO'' AND destination = ''',
      h.final_destination, ''' AND scheduled_time >= TIMESTAMP ''',
      DATE_FORMAT(h.impacted_at - INTERVAL '1' HOUR, 'yyyy-MM-dd HH:mm:ss'),
      ''' ORDER BY scheduled_time ASC LIMIT 12',
      '; hotel_query=SELECT * FROM `hotel_inventory` LIMIT 10'
    ),
    h.`key`
  )) AS r(agent_status, response)
),
offers AS (
  SELECT
    p.passenger_id,
    p.impacted_at,
    o.n,
    NULLIF(REGEXP_EXTRACT(p.response, CONCAT('O', o.n, '_FLIGHT=([^;\s]+)'), 1), 'NONE') AS recommended_flight_id,
    NULLIF(TRIM(REGEXP_EXTRACT(p.response, CONCAT('O', o.n, '_HOTEL=([^;\n]+)'), 1)), 'NONE') AS hotel_name,
    TRY_CAST(REGEXP_EXTRACT(p.response, CONCAT('O', o.n, '_COST=([0-9.]+)'), 1) AS DECIMAL(10, 2)) AS hotel_cost
  FROM plans p
  CROSS JOIN UNNEST(ARRAY['1', '2']) AS o(n)
)
SELECT
  CONCAT(passenger_id, '-O', n) AS `key`,
  passenger_id,
  recommended_flight_id,
  hotel_name,
  'OFFERED' AS status,
  hotel_cost,
  impacted_at,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS recommended_at
FROM offers
WHERE recommended_flight_id IS NOT NULL;

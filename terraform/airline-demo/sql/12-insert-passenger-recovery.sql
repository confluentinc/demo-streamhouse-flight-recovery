-- Concierge spoke: read at-risk passengers from the central table, join the available
-- rebooking_inventory for their destination, run the streaming agent, and write one
-- concrete passenger-facing plan.
--
-- AI_RUN_AGENT('agent', <single prompt STRING>, <request_id>) returns
-- ROW<status, response>. We CONCAT the passenger's governed context into one prompt,
-- then parse recovery_type + action out of the agent's structured response with
-- REGEXP_EXTRACT.
--
-- AI_RUN_AGENT is non-deterministic, so Flink cannot invoke it directly on the
-- update/retraction changelog emitted by the maintained-state join. TO_CHANGELOG
-- turns INSERT and UPDATE_AFTER rows into an append-only trigger stream first;
-- UPDATE_BEFORE and DELETE rows intentionally do not call the agent.
-- The request ID includes risk + connection minutes so a new state for the same
-- passenger is a distinct agent invocation and a distinct system-log trace.
INSERT INTO passenger_recovery
WITH recovery_candidates AS (
  SELECT
    j.`key`,
    j.passenger_name,
    j.risk,
    j.destination,
    j.gate,
    j.minutes_to_departure,
    j.needs_recheck,
    rb.next_flight,
    rb.next_departure,
    rb.hotel
  FROM passenger_journey j
  LEFT JOIN rebooking_inventory rb ON j.destination = rb.`key`
  WHERE j.risk IN ('MISS', 'TIGHT')
),
recovery_events AS (
  SELECT *
  FROM TO_CHANGELOG(
    input      => TABLE recovery_candidates PARTITION BY `key`,
    op         => DESCRIPTOR(change_type),
    op_mapping => MAP['INSERT, UPDATE_AFTER', 'UPSERT']
  )
)
SELECT
  e.`key`,
  REGEXP_EXTRACT(r.response, 'TYPE=([A-Z_]+)', 1)  AS recovery_type,
  REGEXP_EXTRACT(r.response, 'ACTION=(.*)$', 1)    AS action,
  'PROPOSED'                                       AS status
FROM recovery_events e,
LATERAL TABLE(AI_RUN_AGENT(
  'recovery_agent',
  CONCAT(
    'instruction=Use only this current passenger state. Replace any earlier proposal when risk changes; ',
    'For REBOOK, include the exact next_flight, next_departure, and hotel values; ',
    'passenger_name=', COALESCE(e.passenger_name, 'the passenger'),
    '; risk=', e.risk,
    '; destination=', COALESCE(e.destination, 'unknown'),
    '; gate=', COALESCE(e.gate, 'unknown'),
    '; minutes_to_departure=', CAST(e.minutes_to_departure AS STRING),
    '; needs_recheck=', CAST(e.needs_recheck AS STRING),
    '; next_flight=', COALESCE(e.next_flight, 'none'),
    '; next_departure=', COALESCE(CAST(e.next_departure AS STRING), 'none'),
    '; hotel=', COALESCE(e.hotel, 'none')
  ),
  CONCAT(e.`key`, '-', e.risk, '-', CAST(e.minutes_to_departure AS STRING))
)) AS r(agent_status, response);

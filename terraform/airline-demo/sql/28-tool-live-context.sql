-- The agent's live context: the Real-Time Context Engine MCP server. `uv run deploy`
-- creates 'rtce-connection' with the Confluent CLI (streamable HTTP + the Global key), so
-- no secret appears in this statement.
CREATE TOOL IF NOT EXISTS live_context
USING CONNECTION `rtce-connection`
WITH (
  'type' = 'mcp',
  'allowed_tools' = 'getMetadata,queryData',
  'description' = 'River Air live data: flight_status for flight times, hotel_inventory for rooms and nightly rates.',
  'request_timeout' = '30'
);

-- Recovery text-generation model, backed by the core managed Bedrock connection
-- ('bedrock-connection', created in terraform/core). Claude Sonnet on Bedrock.
--
-- Uses lowercase 'bedrock.params.*' (the casing current Confluent AI docs use).
CREATE MODEL IF NOT EXISTS recovery_model
INPUT (prompt STRING)
OUTPUT (response STRING)
WITH (
  'provider' = 'bedrock',
  'task' = 'text_generation',
  'bedrock.connection' = 'bedrock-connection',
  'bedrock.params.max_tokens' = '2048'
);

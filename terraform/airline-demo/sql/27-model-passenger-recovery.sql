-- Claude on Amazon Bedrock, through core's 'bedrock-connection'. Streaming Agents support
-- Anthropic, Gemini, and OpenAI models, not the Confluent-hosted open models.
CREATE MODEL IF NOT EXISTS passenger_recovery_model
INPUT (prompt STRING)
OUTPUT (response STRING)
WITH (
  'provider' = 'bedrock',
  'task' = 'text_generation',
  'bedrock.connection' = 'bedrock-connection',
  'bedrock.params.max_tokens' = '1024'
);

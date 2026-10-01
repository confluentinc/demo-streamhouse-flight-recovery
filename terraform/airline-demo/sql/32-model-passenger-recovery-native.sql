-- Claude hosted in Confluent (Flink Native Inference), for scene 2.3a. Nothing uses it yet:
-- Native Inference models can't call tools, so the agent in sql/30 still runs on the Bedrock
-- model from sql/28. When tool calling works, point the agent here and drop sql/28.
CREATE MODEL IF NOT EXISTS passenger_recovery_mode1
INPUT (prompt STRING)
OUTPUT (response STRING)
WITH (
  'provider' = 'confluent',
  'task' = 'text_generation',
  'confluent.model' = 'anthropic.claude-sonnet-4-6',
  'confluent.input_format' = 'confluent-chat',
  'confluent.params.max_tokens' = '1024'
);

# Change Log

## Unreleased (`dev`)

### Features and Fixes

- Initial demo: Terraform for Confluent Cloud, Flink SQL pipeline, seeded airline data generator, RTCE/Lightning Tables setup, operations + passenger app, and Tableflow walkthrough.
- Kafka Streams alongside Flink for the app: `kstreams/` applies the same connection-risk rules as `sql/24`–`25` in a Java service (3 instances, KIP-1071, exactly-once) and serves the dashboard from its state stores. `uv run deploy` starts it on the demo's topics, and the dashboard runs twice, powered by Flink (:8000) and by Kafka Streams (:8001), side by side at `/compare`.

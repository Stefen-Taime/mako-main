# Kafka Sink Examples

Pipelines that publish events to Apache Kafka topics.

## Pipelines

| Pipeline | Source | Transforms | Description |
|----------|--------|------------|-------------|
| [pipeline-http-to-kafka.yaml](pipeline-http-to-kafka.yaml) | HTTP API | Rename, cast, drop | Fetch from API, validate with Schema Registry, publish to Kafka |

## Features

- **Schema Registry integration** -- Validate events before publishing
- **DLQ support** -- Failed events routed to dead-letter topic
- **Batching** -- Configurable batch size and flush interval
- **Slack alerts** -- Real-time alert notifications

## Key Config Options

```yaml
sink:
  type: kafka
  topic: my-events
  config:
    brokers: ${KAFKA_BROKERS:-localhost:9092}
  batch:
    size: 500
    interval: 10s

schema:
  enforce: true
  registry: http://localhost:8081
  subject: my-events-value
  compatibility: BACKWARD
  onFailure: dlq
  dlqTopic: my-events.dlq
```

## Prerequisites

- Kafka broker running
- Schema Registry (optional, for schema validation)

## Run

```bash
mako validate examples/sinks/kafka/pipeline-http-to-kafka.yaml
mako run examples/sinks/kafka/pipeline-http-to-kafka.yaml
```

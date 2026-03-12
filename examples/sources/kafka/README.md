# Kafka Source Examples

Pipelines that consume events from Apache Kafka topics.

## Pipelines

| Pipeline | Topic | Transforms | Sink | Description |
|----------|-------|------------|------|-------------|
| [pipeline-order-events.yaml](pipeline-order-events.yaml) | events.orders | PII hash, drop, filter | Snowflake | Order events with PII masking |
| [pipeline-payment-features.yaml](pipeline-payment-features.yaml) | events.payments | - | PostgreSQL | Payment event stream |

## Key Config Options

```yaml
source:
  type: kafka
  topic: events.orders
  brokers: ${KAFKA_BROKERS:-localhost:9092}
  startOffset: earliest    # earliest | latest
```

## Prerequisites

- Kafka broker running (see `docker/` for local setup)
- Topic created with appropriate partitions

## Run

```bash
mako validate examples/sources/kafka/pipeline-order-events.yaml
mako run examples/sources/kafka/pipeline-order-events.yaml
```

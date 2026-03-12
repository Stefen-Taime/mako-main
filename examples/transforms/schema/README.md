# Schema Validation Examples

Pipelines that enforce data schemas using Confluent Schema Registry.

## Pipelines

| Pipeline | On Failure | Description |
|----------|------------|-------------|
| [pipeline-beer-schema.yaml](pipeline-beer-schema.yaml) | log | Log invalid events and continue |
| [pipeline-beer-schema-strict.yaml](pipeline-beer-schema-strict.yaml) | reject | Reject invalid events (pipeline stops) |
| [pipeline-beer-schema-dlq.yaml](pipeline-beer-schema-dlq.yaml) | dlq | Route invalid events to a dead-letter queue |

## Failure Modes

- **log** -- Log the validation error and skip the event
- **reject** -- Stop the pipeline on first invalid event
- **dlq** -- Route invalid events to a DLQ topic for later inspection

## Key Config Options

```yaml
schema:
  enforce: true
  registry: http://localhost:8081
  subject: beer-value
  compatibility: BACKWARD         # BACKWARD | FORWARD | FULL | NONE
  onFailure: dlq                  # log | reject | dlq
  dlqTopic: beer-events.dlq      # required when onFailure: dlq
```

## Prerequisites

- Confluent Schema Registry running (see `docker/` for local setup)
- Schema registered for the subject

## Run

```bash
mako validate examples/transforms/schema/pipeline-beer-schema.yaml
mako run examples/transforms/schema/pipeline-beer-schema.yaml
```

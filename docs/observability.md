# Observability

When running `mako run`, Mako performs **preflight checks** (verifying source reachability and sink connectivity) before processing any data. It then exposes real-time Prometheus metrics, health probes, and pipeline status via an HTTP server.

> **Tip:** Run `mako init --full pipeline.yaml` to get a template with all monitoring and alerting options pre-configured.

```yaml
monitoring:
  freshnessSLA: 5m
  alertChannel: "#data-alerts"
  metrics:
    enabled: true
    port: 9090
```

## Endpoints

| Endpoint | Description | Use |
|----------|-------------|-----|
| `GET /metrics` | Prometheus text format | Scraping by Prometheus/Grafana |
| `GET /health` | Liveness probe (always 200) | Kubernetes `livenessProbe` |
| `GET /ready` | Readiness probe (200 when pipeline running) | Kubernetes `readinessProbe` |
| `GET /status` | Pipeline status JSON | Monitoring dashboards |

## Prometheus Metrics

```text
mako_events_in_total{pipeline="order-events"} 15234
mako_events_out_total{pipeline="order-events"} 15230
mako_errors_total{pipeline="order-events"} 4
mako_dlq_total{pipeline="order-events"} 2
mako_schema_failures_total{pipeline="order-events"} 1
mako_sink_latency_microseconds{pipeline="order-events"} 4523
mako_throughput_events_per_second{pipeline="order-events"} 1523.40
mako_uptime_seconds{pipeline="order-events"} 3600.0
mako_pipeline_ready{pipeline="order-events"} 1
```

### Real-time metrics

Metrics are synced from the pipeline engine to the observability server every 500ms. The `/metrics` and `/status` endpoints reflect live counters during pipeline execution, not just final stats at shutdown. A final sync is performed after `pipeline.Stop()` to capture events flushed during graceful shutdown.

## Kubernetes Probes

```yaml
# Example Kubernetes probe configuration
livenessProbe:
  httpGet:
    path: /health
    port: 9090
readinessProbe:
  httpGet:
    path: /ready
    port: 9090
```

## Schema Enforcement

Validate events against Confluent Schema Registry at runtime. Supports **JSON Schema** and **Avro** schema types.

```yaml
schema:
  enforce: true
  registry: http://schema-registry:8081
  subject: events.orders-value
  compatibility: BACKWARD
  onFailure: reject     # reject | dlq | log
  dlqTopic: events.orders.dlq
```

### JSON Schema

- Type checking (string, number, integer, boolean, object, array, null)
- Required fields enforcement
- `additionalProperties: false` support

### Avro

Powered by [hamba/avro](https://github.com/hamba/avro) (pure Go, high performance).

- Full record validation (field types, required fields, defaults)
- Numeric type coercion (JSON `float64` -> Avro `int`, `long`, `float`)
- Union types (`["null", "string"]`)
- Nested records and arrays
- Enum validation

Example Avro schema in the registry:

```json
{
  "type": "record",
  "name": "OrderEvent",
  "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "amount", "type": "double"},
    {"name": "quantity", "type": "int"},
    {"name": "status", "type": ["null", "string"], "default": null}
  ]
}
```

### Common features

- Schema caching (fetched once, refreshable)
- Failed events routed to DLQ or rejected
- Automatic schema type detection from Registry (`schemaType` field)

## Slack Alerting

Send pipeline alerts to a Slack channel via incoming webhook. Mako notifies on errors, SLA breaches, and pipeline completions.

```yaml
monitoring:
  freshnessSLA: 5m
  alertChannel: "#data-alerts"
  slackWebhookURL: ${SLACK_WEBHOOK_URL}
  alertOnError: true       # default: true
  alertOnSLA: true         # default: true
  alertOnComplete: true    # default: false
  metrics:
    enabled: true
    port: 9090
```

### Configuration

| Key | Default | Description |
|---|---|---|
| `slackWebhookURL` | — | Slack incoming webhook URL. Supports `${ENV_VAR}` expansion. Falls back to `SLACK_WEBHOOK_URL` env var |
| `alertChannel` | — | Slack channel (e.g., `#data-alerts`). Passed in the webhook payload |
| `alertOnError` | `true` | Send alert when pipeline errors occur |
| `alertOnSLA` | `true` | Send alert when `freshnessSLA` is breached (time since last event exceeds threshold) |
| `alertOnComplete` | `false` | Send summary alert when pipeline completes |

If `slackWebhookURL` is empty and `SLACK_WEBHOOK_URL` is not set, alerting is silently disabled. All webhook calls are asynchronous (goroutine) with a 5s timeout — failures are logged to stderr but never crash the pipeline.

### Alert types

**Error alert** (red):
- Triggered when the pipeline error counter increases
- Includes: error message, events in/out counts

**SLA breach alert** (red):
- Triggered once when `time.Since(lastEvent) > freshnessSLA`
- Includes: configured SLA, actual delay
- Sent at most once per pipeline run (no spam)

**Completion alert** (green/orange):
- Triggered when the pipeline shuts down
- Green if 0 errors, orange if errors > 0
- Includes: events in/out, error count, total duration

### Message format

Alerts use Slack Block Kit attachments with color coding:

| Condition | Color |
|---|---|
| Error / SLA breach | Red (`#dc3545`) |
| Completion (0 errors) | Green (`#28a745`) |
| Completion (with errors) | Orange (`#fd7e14`) |

Messages are posted as "Mako Pipeline" with a :shark: emoji.

### Setup

1. Create a [Slack Incoming Webhook](https://api.slack.com/messaging/webhooks) for your workspace
2. Set the URL as an environment variable:
   ```bash
   export SLACK_WEBHOOK_URL=https://hooks.slack.com/services/T00/B00/xxx
   ```
3. Or configure it directly in the pipeline YAML:
   ```yaml
   monitoring:
     slackWebhookURL: https://hooks.slack.com/services/T00/B00/xxx
   ```

## Alert Rules

Define threshold-based alert rules in the `monitoring.alerts` section. Rules are evaluated every 10 seconds and trigger Slack notifications when a threshold is breached. Each rule has a 5-minute cooldown to prevent notification spam.

```yaml
monitoring:
  freshnessSLA: 5m
  alertChannel: "#data-alerts"
  slackWebhookURL: ${SLACK_WEBHOOK_URL}
  alerts:
    - name: payment_latency
      type: latency
      threshold: 30s
      severity: critical
      channel: "#fraud-incidents"     # override alertChannel

    - name: high_error_rate
      type: error_rate
      threshold: "0.5%"
      severity: critical

    - name: volume_drop
      type: volume
      threshold: "-50%"
      severity: warning
```

### Rule types

| Type | Threshold format | Trigger condition | Example |
|---|---|---|---|
| `latency` | Duration (`30s`, `5m`) | Time since last event exceeds threshold | `threshold: 30s` |
| `error_rate` | Percentage (`0.5%`, `1%`) | `errors / eventsIn` exceeds threshold | `threshold: "0.5%"` |
| `volume` | Signed percentage (`-50%`, `+200%`) | Throughput rate change exceeds threshold | `threshold: "-50%"` |

**Latency** measures the time since the last event was received. If no events arrive for longer than the threshold, the rule fires. This is useful for detecting stale data or source failures.

**Error rate** computes `errors / eventsIn` as a percentage. A threshold of `"0.5%"` triggers when more than 0.5% of ingested events result in errors.

**Volume** compares the current event throughput to the previous measurement interval. A threshold of `"-50%"` triggers when the event rate drops by more than 50%. A threshold of `"+200%"` triggers when the rate increases by more than 200% (useful for detecting traffic spikes).

### Severity and colors

| Severity | Slack color | Use case |
|---|---|---|
| `critical` | Red (`#dc3545`) | Immediate attention required |
| `warning` | Orange (`#fd7e14`) | Degraded performance, investigate soon |
| `info` | Blue (`#2196F3`) | Informational, no action needed |

### Cooldown

Each rule has a 5-minute cooldown after firing. During the cooldown period, the rule is still evaluated but won't send duplicate notifications. This prevents notification flooding during sustained threshold breaches.

### Channel override

Each rule can specify its own `channel` field to override the default `alertChannel`. This is useful for routing critical alerts to incident channels while keeping informational alerts in a general channel.

### Evaluation

Rules are evaluated every 10 seconds (not every 500ms like metrics sync) to avoid excessive API calls and computation. Rule triggers are logged to stderr:

```text
[alert] rule "payment_latency" triggered: latency 45s exceeds threshold 30s
[alert] rule "high_error_rate" triggered: error rate 1.20% exceeds threshold 0.50% (12 errors / 1000 events)
```

## Fault Isolation

Each pipeline runs independently. Failed events go to a Dead Letter Queue instead of blocking the pipeline.

```yaml
isolation:
  strategy: per_event_type
  maxRetries: 3
  backoffMs: 1000
  dlqEnabled: true
```

Retry flow: sink write failure -> exponential backoff -> retry (up to N times) -> DLQ -> degrade state.

### DLQ topic

The DLQ topic name defaults to `<source.topic>.dlq` or can be set explicitly via `schema.dlqTopic`. The DLQ producer is configured with `AllowAutoTopicCreation`, so the topic is created automatically if the Kafka broker allows it (`auto.create.topics.enable=true`, which is the default). If your broker has auto-creation disabled, create the topic manually before running the pipeline:

```bash
kafka-topics --create --topic events.orders.dlq \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1
```

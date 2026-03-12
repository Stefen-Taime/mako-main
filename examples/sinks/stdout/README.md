# Stdout Sink Examples

Pipelines that output transformed events to the console (useful for development and debugging).

## Pipelines

| Pipeline | Source | Transforms | Description |
|----------|--------|------------|-------------|
| [pipeline-wasm-stdout.yaml](pipeline-wasm-stdout.yaml) | Kafka | PII hash, WASM plugin, filter | Debug WASM enrichment pipeline output |

## Key Config Options

```yaml
sink:
  type: stdout
```

The stdout sink prints each event as a JSON line to standard output. Useful for:

- Debugging transform chains
- Validating WASM plugin output
- Testing pipeline logic without a real sink

## Run

```bash
mako validate examples/sinks/stdout/pipeline-wasm-stdout.yaml
mako run examples/sinks/stdout/pipeline-wasm-stdout.yaml
```

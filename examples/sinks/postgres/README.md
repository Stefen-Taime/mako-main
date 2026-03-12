# PostgreSQL Sink Examples

Pipelines that write data to PostgreSQL tables.

## Pipelines

| Pipeline | Source | Features | Description |
|----------|--------|----------|-------------|
| [pipeline-http-to-postgres.yaml](pipeline-http-to-postgres.yaml) | HTTP API | Flatten, batching | Beer API data loaded to PostgreSQL |
| [pipeline-beer-vault.yaml](pipeline-beer-vault.yaml) | HTTP API | Vault secrets, flatten | Credentials resolved from HashiCorp Vault |

## Features

- **Auto-flatten** -- Nested JSON objects are automatically flattened to typed columns
- **Batch inserts** -- Configurable batch size and flush interval
- **COPY protocol** -- Uses PostgreSQL COPY for high-throughput loading
- **Vault integration** -- Credentials can be resolved from HashiCorp Vault

## Key Config Options

```yaml
sink:
  type: postgres
  database: mako
  schema: public
  table: my_table
  flatten: true           # auto-create columns from JSON structure
  batch:
    size: 500
    interval: 5s
```

## Vault Integration

```yaml
vault:
  path: secret/data/mako
  ttl: 5m

sink:
  type: postgres
  config:
    vault_path: secret/data/mako/postgres   # host, port, user, password from Vault
```

## Prerequisites

- PostgreSQL instance running (see `docker/` for local setup)

## Run

```bash
mako validate examples/sinks/postgres/pipeline-http-to-postgres.yaml
mako run examples/sinks/postgres/pipeline-http-to-postgres.yaml
```

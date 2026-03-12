# Snowflake Sink Examples

Pipelines that write data to Snowflake tables.

## Pipelines

| Pipeline | Source | Transforms | Description |
|----------|--------|------------|-------------|
| [pipeline-movies.yaml](pipeline-movies.yaml) | File (JSON) | Filter, drop | Movies dataset to Snowflake with auto-flatten |
| [pipeline-users.yaml](pipeline-users.yaml) | File (JSON) | - | User profiles to Snowflake |

## Features

- **Auto-flatten** -- Nested JSON objects mapped to typed Snowflake columns
- **Batch loading** -- Configurable batch size with periodic flush
- **Auto-DDL** -- Table and columns created automatically from data

## Key Config Options

```yaml
sink:
  type: snowflake
  database: ANALYTICS
  schema: RAW
  table: MY_TABLE
  flatten: true
  config:
    account: REPLACE_WITH_YOUR_ACCOUNT
    user: REPLACE_WITH_YOUR_USER
    warehouse: COMPUTE_WH
    role: ACCOUNTADMIN
  batch:
    size: 1000
    interval: 10s
```

## Prerequisites

- Snowflake account with appropriate permissions
- Environment variables or config for authentication

## Run

```bash
mako validate examples/sinks/snowflake/pipeline-movies.yaml
mako run examples/sinks/snowflake/pipeline-movies.yaml
```

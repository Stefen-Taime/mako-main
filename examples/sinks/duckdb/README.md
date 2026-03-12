# DuckDB Sink Examples

Pipelines that write data to embedded DuckDB databases.

## Pipelines

| Pipeline | Source | Features | Description |
|----------|--------|----------|-------------|
| [pipeline-beer-to-duckdb.yaml](pipeline-beer-to-duckdb.yaml) | HTTP API | Auto-table creation | Beer API data loaded to local DuckDB file |
| [pipeline-parquet-to-duckdb.yaml](pipeline-parquet-to-duckdb.yaml) | DuckDB (GCS Parquet) | Cloud storage read | Read from GCS Parquet, write to DuckDB |

## Features

- **Auto-table creation** -- Table created automatically from data schema
- **Schema evolution** -- New columns added dynamically
- **Export to Parquet/CSV** -- DuckDB COPY TO for file export
- **Partitioned export** -- Export with partition_by columns

## Key Config Options

```yaml
sink:
  type: duckdb
  database: /tmp/analytics.duckdb
  table: events
  config:
    create_table: true
    export_path: /data/output/         # optional: COPY TO export
    export_format: parquet             # parquet | csv
    export_partition_by: [year, month] # optional partitioning
  batch:
    size: 500
    interval: 5s
```

## Run

```bash
mako validate examples/sinks/duckdb/pipeline-beer-to-duckdb.yaml
mako run examples/sinks/duckdb/pipeline-beer-to-duckdb.yaml
```

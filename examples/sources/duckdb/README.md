# DuckDB Source Examples

Pipelines that read data from DuckDB databases or use DuckDB's native file readers.

## Pipelines

| Pipeline | Input | Sink | Description |
|----------|-------|------|-------------|
| [pipeline-parquet-to-duckdb.yaml](pipeline-parquet-to-duckdb.yaml) | Parquet (GCS) | DuckDB | Read Parquet from cloud storage into DuckDB |
| [pipeline-duckdb-to-parquet.yaml](pipeline-duckdb-to-parquet.yaml) | DuckDB table | DuckDB (export) | Export DuckDB table to partitioned Parquet files |
| [pipeline-duckdb-to-local-parquet.yaml](pipeline-duckdb-to-local-parquet.yaml) | DuckDB table | DuckDB (export) | Export DuckDB data to local Parquet |

## Key Config Options

```yaml
source:
  type: duckdb
  config:
    database: /data/analytics.duckdb
    query: "SELECT * FROM events WHERE year = 2024"
```

DuckDB can natively read from cloud storage (S3, GCS, Azure) using the httpfs extension:

```yaml
source:
  type: duckdb
  config:
    database: ":memory:"
    query: "SELECT * FROM read_parquet('gs://bucket/path/*.parquet')"
    gcs_project: my-project
```

## Run

```bash
mako validate examples/sources/duckdb/pipeline-duckdb-to-parquet.yaml
mako run examples/sources/duckdb/pipeline-duckdb-to-parquet.yaml
```

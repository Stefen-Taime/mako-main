# Multi-Source Demo Workflow

A workflow demonstrating parallel and sequential pipeline execution with multiple sources and sinks.

## DAG

```text
ingest-users ---------> users-to-postgres
     (HTTP -> DuckDB)       (DuckDB -> PostgreSQL)

ingest-restaurants      (runs in parallel with ingest-users)
     (CSV -> DuckDB)
```

## Files

| File | Type | Description |
|------|------|-------------|
| [workflow.yaml](workflow.yaml) | Workflow | DAG definition with parallel + sequential steps |
| [pipeline-users.yaml](pipeline-users.yaml) | Pipeline | HTTP API to DuckDB + Parquet export |
| [pipeline-users-to-postgres.yaml](pipeline-users-to-postgres.yaml) | Pipeline | DuckDB/Parquet to PostgreSQL |
| [pipeline-restaurants.yaml](pipeline-restaurants.yaml) | Pipeline | CSV file to DuckDB + Parquet export |

## Run

```bash
mako workflow examples/workflows/multi-source-demo/workflow.yaml
```

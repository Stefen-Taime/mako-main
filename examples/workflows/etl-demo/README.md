# ETL Demo Workflow

A simple 3-step sequential workflow demonstrating basic DAG orchestration.

## DAG

```text
ingest --> transform --> load
```

## Steps

| Step | Pipeline | Description |
|------|----------|-------------|
| ingest | Parquet to DuckDB | Ingest raw data from Parquet into DuckDB |
| transform | Simple pipeline | Apply transforms to data |
| load | Users to Snowflake | Load transformed data to Snowflake |

## Run

```bash
mako workflow examples/workflows/etl-demo/workflow.yaml
```

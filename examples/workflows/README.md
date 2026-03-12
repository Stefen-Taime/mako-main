# Workflows

DAG-based orchestration of multiple pipelines. Each subfolder contains a complete workflow with its pipeline files.

| Workflow | Steps | Features | Description |
|----------|-------|----------|-------------|
| [NYC TLC Star Schema](nyc-tlc-star-schema/) | 9 + quality gate | Star schema, DuckDB, quality gate | Build a dimensional model from 700K+ NYC taxi trip records |
| [ETL Demo](etl-demo/) | 3 | Sequential DAG | Simple ingest -> transform -> load pipeline chain |
| [Multi-Source Demo](multi-source-demo/) | 3 | Parallel execution | HTTP + CSV sources, DuckDB + PostgreSQL sinks |

## How Workflows Work

A workflow is a DAG of pipeline steps. Steps with no dependencies run in parallel; downstream steps wait for their dependencies:

```yaml
apiVersion: mako/v1
kind: Workflow

workflow:
  name: my-workflow
  onFailure: stop          # stop | continue

  steps:
    - name: step-a
      pipeline: pipeline-a.yaml

    - name: step-b
      pipeline: pipeline-b.yaml
      depends_on: [step-a]

    - name: quality-check
      type: quality_gate
      database: /tmp/warehouse.duckdb
      depends_on: [step-b]
      checks:
        - name: "table has data"
          sql: "SELECT COUNT(*) FROM my_table"
          expect: "> 0"
```

## Shared Metrics

When running a workflow, all pipelines expose metrics on a single shared Prometheus endpoint (`:9090`) instead of separate ports.

See [docs/workflows.md](../../docs/workflows.md) for full documentation.

# SQL Transform Examples

Pipelines that use the built-in `sql` transform to enrich events with SQL expressions.

## Pipelines

| Pipeline | Description |
|----------|-------------|
| [pipeline-beer-sql-transform.yaml](pipeline-beer-sql-transform.yaml) | CASE WHEN to categorize beer strength (light/medium/strong) |
| [pipeline-gcs-to-duckdb-analytics.yaml](pipeline-gcs-to-duckdb-analytics.yaml) | SQL enrichment for analytics (fare categories, computed fields) |

## How It Works

The SQL transform runs each event through an in-memory DuckDB query. Reference the incoming record as `events`:

```yaml
transforms:
  - name: enrich
    type: sql
    query: >
      SELECT *,
        CASE WHEN amount > 100 THEN 'high'
             WHEN amount > 50  THEN 'medium'
             ELSE 'low'
        END as tier
      FROM events
```

## Capabilities

- `SELECT *` to keep all original fields + add computed columns
- `CASE WHEN` for conditional logic
- All DuckDB SQL functions available (string, math, date, etc.)
- `WHERE` clause for row-level filtering

## Run

```bash
mako validate examples/transforms/sql/pipeline-beer-sql-transform.yaml
mako run examples/transforms/sql/pipeline-beer-sql-transform.yaml
```

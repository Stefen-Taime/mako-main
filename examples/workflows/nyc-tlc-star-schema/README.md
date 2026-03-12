# NYC TLC Star Schema Workflow

A complete workflow that builds a dimensional model (star schema) from NYC Taxi & Limousine Commission public Parquet data.

## Architecture

```text
  dim-vendor
       |
  dim-payment
       |
  dim-rate-code
       |
  dim-date
       |
  dim-time
       |
  dim-location
       |
  fact-trips          (~700K rows)
       |
  agg-daily           (daily revenue aggregation)
       |
  quality-gate        (SQL assertions on final warehouse)
```

## Files

| File | Type | Description |
|------|------|-------------|
| [workflow.yaml](workflow.yaml) | Workflow | DAG definition with 9 pipeline steps + quality gate |
| [pipeline-dim-vendor.yaml](pipeline-dim-vendor.yaml) | Pipeline | dim_vendor dimension (3 rows) |
| [pipeline-dim-payment.yaml](pipeline-dim-payment.yaml) | Pipeline | dim_payment_type dimension (4 rows) |
| [pipeline-dim-rate-code.yaml](pipeline-dim-rate-code.yaml) | Pipeline | dim_rate_code dimension (6 rows) |
| [pipeline-dim-date.yaml](pipeline-dim-date.yaml) | Pipeline | dim_date dimension (366 rows) |
| [pipeline-dim-time.yaml](pipeline-dim-time.yaml) | Pipeline | dim_time dimension (24 rows) |
| [pipeline-dim-location.yaml](pipeline-dim-location.yaml) | Pipeline | dim_location dimension (259 zones) |
| [pipeline-fact-trips.yaml](pipeline-fact-trips.yaml) | Pipeline | fact_trips fact table (~700K rows) |
| [pipeline-agg-daily.yaml](pipeline-agg-daily.yaml) | Pipeline | agg_daily_summary aggregation |
| [pipeline-tlc-to-gcs.yaml](pipeline-tlc-to-gcs.yaml) | Pipeline | Raw Parquet ingest to GCS |
| [pipeline-gcs-to-duckdb-analytics.yaml](pipeline-gcs-to-duckdb-analytics.yaml) | Pipeline | GCS Parquet to DuckDB analytics |

## Quality Gate

The workflow ends with SQL assertions against the final DuckDB warehouse:

- `fact_trips` has > 100K rows
- All 6 dimensions are populated
- `agg_daily_summary` has data
- No negative revenue in aggregation

## Output

```
/tmp/nyc_tlc_warehouse.duckdb
```

## Run

```bash
mako workflow examples/workflows/nyc-tlc-star-schema/workflow.yaml
```

Typical runtime: ~9 minutes (downloads ~400MB Parquet file from NYC TLC public dataset).

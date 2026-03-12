# GCS Sink Examples

Pipelines that write data to Google Cloud Storage buckets.

## Pipelines

| Pipeline | Source | Format | Description |
|----------|--------|--------|-------------|
| [pipeline-tlc-to-gcs.yaml](pipeline-tlc-to-gcs.yaml) | File (Parquet) | Parquet | NYC TLC taxi data with dq_check quality shield |
| [pipeline-postgres-to-gcs.yaml](pipeline-postgres-to-gcs.yaml) | PostgreSQL CDC | Parquet | Snapshot PostgreSQL table to GCS Parquet |
| [pipeline-openfoodfacts-to-gcs.yaml](pipeline-openfoodfacts-to-gcs.yaml) | File (CSV gzip) | CSV | OpenFoodFacts product catalog (streaming decompression) |

## Supported Formats

- **Parquet** -- Columnar format with Snappy compression
- **CSV** -- Plain text CSV files

## Key Config Options

```yaml
sink:
  type: gcs
  bucket: my-bucket
  prefix: raw/data/2024/01
  format: parquet
  config:
    project: my-gcp-project
    compression: snappy       # snappy | gzip | none
  batch:
    size: 5000
    interval: 10s
```

## Prerequisites

- GCP project with a GCS bucket
- Application Default Credentials configured (`gcloud auth application-default login`)

## Run

```bash
mako validate examples/sinks/gcs/pipeline-tlc-to-gcs.yaml
mako run examples/sinks/gcs/pipeline-tlc-to-gcs.yaml
```

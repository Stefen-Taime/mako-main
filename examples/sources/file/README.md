# File Source Examples

Pipelines that ingest data from local or remote files using the `file` source type.

## Pipelines

| Pipeline | Format | Source | Sink | Description |
|----------|--------|--------|------|-------------|
| [pipeline-commerce.yaml](pipeline-commerce.yaml) | JSON | GitHub raw URL | PostgreSQL | Commerce/product data |
| [pipeline-movies.yaml](pipeline-movies.yaml) | JSON | GitHub raw URL | PostgreSQL | Movies dataset |
| [pipeline-users.yaml](pipeline-users.yaml) | JSON | GitHub raw URL | PostgreSQL | User profiles |
| [pipeline-stripe.yaml](pipeline-stripe.yaml) | JSON | GitHub raw URL | PostgreSQL | Stripe payment data |
| [pipeline-openfoodfacts-to-gcs.yaml](pipeline-openfoodfacts-to-gcs.yaml) | CSV (gzip) | HTTP URL | GCS | OpenFoodFacts product catalog (tab-delimited, streaming gzip decompression) |

## Supported Formats

- **JSON** -- Single array or newline-delimited JSON
- **JSONL** -- One JSON object per line
- **CSV** -- With configurable delimiter and header detection
- **Parquet** -- Native columnar reading (local or HTTP)
- **gzip** -- Transparent streaming decompression for any format

## Key Config Options

```yaml
source:
  type: file
  config:
    path: https://example.com/data.csv.gz   # local path or HTTP URL
    format: csv                              # json | jsonl | csv | parquet
    csv_header: true
    csv_delimiter: ","
    channel_buffer: 20000                    # buffer size for large files
```

## Run

```bash
mako validate examples/sources/file/pipeline-commerce.yaml
mako run examples/sources/file/pipeline-commerce.yaml
```
